/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use byteorder::NetworkEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use std::collections::BTreeMap;
use std::fmt;
use std::io::Read;
use std::io::Write;
use std::iter::FromIterator;
use std::vec::Vec;

use crate::error::*;
use crate::sasl::*;
use crate::types::*;

#[derive(Debug)]
pub struct FrameHeader {
    pub size: u32,
    doff: u8,
    frame_type: u8,
    ext: u16,
}

#[derive(Debug)]
pub enum Frame {
    AMQP(AmqpFrame),
    SASL(SaslFrame),
}

#[derive(Debug)]
pub struct AmqpFrame {
    pub channel: u16,
    pub body: Option<Performative>,
}

#[derive(Debug)]
pub enum SaslFrame {
    SaslMechanisms(SaslMechanisms),
    SaslInit(SaslInit),
    SaslChallenge(SaslChallenge),
    SaslResponse(SaslResponse),
    SaslOutcome(SaslOutcome),
}

type SaslMechanisms = Vec<SaslMechanism>;

#[derive(Debug)]
pub struct SaslInit {
    pub mechanism: String,
    pub initial_response: Option<Vec<u8>>,
    pub hostname: Option<String>,
}

pub type SaslChallenge = Vec<u8>;
pub type SaslResponse = Vec<u8>;

#[derive(Debug)]
pub struct SaslOutcome {
    pub code: SaslCode,
    additional_data: Option<Vec<u8>>,
}

pub type SaslCode = u8;

impl Encoder for SaslInit {
    fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        let val = vec![
            Value::Symbol(self.mechanism.clone().into_bytes()),
            self.initial_response
                .to_value(|v| Value::Binary(v.to_vec())),
            self.hostname.to_value(|v| Value::String(v.to_string())),
        ];
        Value::Described(Box::new(DESC_SASL_INIT), Box::new(Value::List(val))).encode(writer)
    }
}

#[derive(Debug)]
pub enum Performative {
    Open(Open),
    Close(Close),
    Begin(Begin),
    Attach(Attach),
    End(End),
}

#[derive(Debug, Clone)]
pub struct Open {
    pub container_id: String,
    pub hostname: Option<String>,
    pub max_frame_size: Option<u32>,
    pub channel_max: Option<u16>,
    pub idle_timeout: Option<u32>,
    pub outgoing_locales: Option<Vec<String>>,
    pub incoming_locales: Option<Vec<String>>,
    pub offered_capabilities: Option<Vec<String>>,
    pub desired_capabilities: Option<Vec<String>>,
    pub properties: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone)]
pub struct Begin {
    pub remote_channel: Option<u16>,
    pub next_outgoing_id: u32,
    pub incoming_window: u32,
    pub outgoing_window: u32,
    pub handle_max: Option<u32>,
    pub offered_capabilities: Option<Vec<String>>,
    pub desired_capabilities: Option<Vec<String>>,
    pub properties: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone)]
pub struct Attach {
    pub name: String,
    pub handle: u32,
    pub role: LinkRole,
    pub snd_settle_mode: Option<SenderSettleMode>,
    pub rcv_settle_mode: Option<ReceiverSettleMode>,
    pub source: Option<Source>,
    pub target: Option<Target>,
    pub unsettled: Option<BTreeMap<Value, Value>>,
    pub incomplete_unsettled: Option<bool>,
    pub initial_delivery_count: Option<u32>,
    pub max_message_size: Option<u64>,
    pub offered_capabilities: Option<Vec<String>>,
    pub desired_capabilities: Option<Vec<String>>,
    pub properties: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum LinkRole {
    Sender,
    Receiver,
}

#[derive(Debug, Clone, Copy)]
pub enum SenderSettleMode {
    Unsettled,
    Settled,
    Mixed,
}

#[derive(Debug, Clone, Copy)]
pub enum ReceiverSettleMode {
    First,
    Second,
}

#[derive(Debug, Clone)]
pub struct Source {
    pub address: Option<String>,
    pub durable: Option<TerminusDurability>,
    pub expiry_policy: Option<TerminusExpiryPolicy>,
    pub timeout: Option<u32>,
    pub dynamic: Option<bool>,
    pub dynamic_node_properties: Option<BTreeMap<String, Value>>,
    pub distribution_mode: Option<String>,
    pub filter: Option<BTreeMap<String, Value>>,
    pub default_outcome: Option<Outcome>,
    pub outcomes: Option<Vec<Outcome>>,
    pub capabilities: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy)]
pub enum TerminusDurability {
    None,
    Configuration,
    UnsettledState,
}

#[derive(Debug, Clone, Copy)]
pub enum TerminusExpiryPolicy {
    LinkDetach,
    SessionEnd,
    ConnectionClose,
    Never,
}

impl fmt::Display for TerminusExpiryPolicy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Display for TerminusDurability {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Outcome {
    Accepted,
    Rejected,
    Released,
    Modified,
}

#[derive(Debug, Clone)]
pub struct Target {
    pub address: Option<String>,
    pub durable: Option<TerminusDurability>,
    pub expiry_policy: Option<TerminusExpiryPolicy>,
    pub timeout: Option<u32>,
    pub dynamic: Option<bool>,
    pub dynamic_node_properties: Option<BTreeMap<String, Value>>,
    pub capabilities: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct End {
    pub error: Option<ErrorCondition>,
}

#[derive(Debug, Clone)]
pub struct Close {
    pub error: Option<ErrorCondition>,
}

const DESC_OPEN: Value = Value::Ulong(0x10);
const DESC_BEGIN: Value = Value::Ulong(0x11);
const DESC_ATTACH: Value = Value::Ulong(0x12);
const DESC_SOURCE: Value = Value::Ulong(0x28);
const DESC_TARGET: Value = Value::Ulong(0x29);

const DESC_END: Value = Value::Ulong(0x17);
const DESC_CLOSE: Value = Value::Ulong(0x18);

const DESC_SASL_MECHANISMS: Value = Value::Ulong(0x40);
const DESC_SASL_INIT: Value = Value::Ulong(0x41);
const DESC_SASL_OUTCOME: Value = Value::Ulong(0x44);
const DESC_ERROR: Value = Value::Ulong(0x1D);

impl Open {
    pub fn new(container_id: &str) -> Open {
        Open {
            container_id: container_id.to_string(),
            hostname: None,
            max_frame_size: None,
            channel_max: None,
            idle_timeout: None,
            outgoing_locales: None,
            incoming_locales: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }

    pub fn from_value(value: Value) -> Result<Open> {
        if let Value::List(args) = value {
            let mut it = args.iter();

            let container_id =
                it.next()
                    .and_then(|c| c.try_to_string())
                    .ok_or(AmqpError::decode_error(Some(
                        "Unable to decode mandatory field 'container_id'",
                    )))?;

            let mut open = Open::new(container_id.as_str());
            if let Some(hostname) = it.next() {
                open.hostname = hostname.try_to_string();
            }

            if let Some(max_frame_size) = it.next() {
                open.max_frame_size = max_frame_size.try_to_u32();
            }

            if let Some(channel_max) = it.next() {
                open.channel_max = channel_max.try_to_u16();
            }

            if let Some(idle_timeout) = it.next() {
                open.idle_timeout = idle_timeout.try_to_u32();
            }

            if let Some(outgoing_locales) = it.next() {
                // TODO:
                // println!("OLOC {:?}", outgoing_locales);
            }

            if let Some(incoming_locales) = it.next() {
                // TODO:
                // println!("ILOC {:?}", incoming_locales);
            }

            if let Some(offered_capabilities) = it.next() {
                if let Value::Array(vec) = offered_capabilities {
                    let mut cap = Vec::new();
                    for val in vec.iter() {
                        cap.push(val.to_string())
                    }
                    open.offered_capabilities = Some(cap);
                } else if let Value::Symbol(s) = offered_capabilities {
                    open.offered_capabilities = Some(vec![String::from_utf8(s.to_vec()).unwrap()]);
                }
            }

            if let Some(desired_capabilities) = it.next() {
                if let Value::Array(vec) = desired_capabilities {
                    let mut cap = Vec::new();
                    for val in vec.iter() {
                        cap.push(val.to_string())
                    }
                    open.desired_capabilities = Some(cap);
                } else if let Value::Symbol(s) = desired_capabilities {
                    open.desired_capabilities = Some(vec![String::from_utf8(s.to_vec()).unwrap()]);
                }
            }

            if let Some(properties) = it.next() {
                if let Value::Map(m) = properties {
                    let mut map = BTreeMap::new();
                    for (key, value) in m.iter() {
                        map.insert(key.to_string(), value.clone());
                    }
                    open.properties = Some(map);
                }
            }

            Ok(open)
        } else {
            Err(AmqpError::decode_error(Some(
                "Missing expected arguments for open performative",
            )))
        }
    }
}

impl Begin {
    pub fn new(next_outgoing_id: u32, incoming_window: u32, outgoing_window: u32) -> Begin {
        Begin {
            remote_channel: None,
            next_outgoing_id: next_outgoing_id,
            incoming_window: incoming_window,
            outgoing_window: outgoing_window,
            handle_max: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }

    pub fn from_value(value: Value) -> Result<Begin> {
        if let Value::List(args) = value {
            let mut begin = Begin::new(0, 0, 0);
            let mut it = args.iter();
            if let Some(remote_channel) = it.next() {
                begin.remote_channel = remote_channel.try_to_u16();
            }

            begin.next_outgoing_id =
                it.next()
                    .and_then(|c| c.try_to_u32())
                    .ok_or(AmqpError::decode_error(Some(
                        "Unable to decode mandatory field 'next-outgoing-id'",
                    )))?;

            begin.incoming_window =
                it.next()
                    .and_then(|c| c.try_to_u32())
                    .ok_or(AmqpError::decode_error(Some(
                        "Unable to decode mandatory field 'incoming-window'",
                    )))?;

            begin.outgoing_window =
                it.next()
                    .and_then(|c| c.try_to_u32())
                    .ok_or(AmqpError::decode_error(Some(
                        "Unable to decode mandatory field 'outgoing-window'",
                    )))?;

            if let Some(handle_max) = it.next() {
                begin.handle_max = handle_max.try_to_u32();
            }

            if let Some(offered_capabilities) = it.next() {
                if let Value::Array(vec) = offered_capabilities {
                    let mut cap = Vec::new();
                    for val in vec.iter() {
                        cap.push(val.to_string())
                    }
                    begin.offered_capabilities = Some(cap);
                } else if let Value::Symbol(s) = offered_capabilities {
                    begin.offered_capabilities = Some(vec![String::from_utf8(s.to_vec()).unwrap()]);
                }
            }

            if let Some(desired_capabilities) = it.next() {
                if let Value::Array(vec) = desired_capabilities {
                    let mut cap = Vec::new();
                    for val in vec.iter() {
                        cap.push(val.to_string())
                    }
                    begin.desired_capabilities = Some(cap);
                } else if let Value::Symbol(s) = desired_capabilities {
                    begin.desired_capabilities = Some(vec![String::from_utf8(s.to_vec()).unwrap()]);
                }
            }

            if let Some(properties) = it.next() {
                if let Value::Map(m) = properties {
                    let mut map = BTreeMap::new();
                    for (key, value) in m.iter() {
                        map.insert(key.to_string(), value.clone());
                    }
                    begin.properties = Some(map);
                }
            }

            Ok(begin)
        } else {
            Err(AmqpError::decode_error(Some(
                "Missing expected arguments for begin performative",
            )))
        }
    }
}

impl Attach {
    pub fn new(name: &str, handle: u32, role: LinkRole) -> Attach {
        Attach {
            name: name.to_string(),
            handle: handle,
            role: role,
            snd_settle_mode: None,
            rcv_settle_mode: None,
            source: None,
            target: None,
            unsettled: None,
            incomplete_unsettled: None,
            initial_delivery_count: None,
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }
}

fn decode_condition(value: Value) -> Result<ErrorCondition> {
    if let Value::Described(descriptor, list) = value {
        match *descriptor {
            DESC_ERROR => {
                if let Value::List(args) = *list {
                    let mut it = args.iter();
                    let mut error_condition = ErrorCondition {
                        condition: String::new(),
                        description: String::new(),
                    };

                    if let Some(condition) = it.next() {
                        error_condition.condition = condition.to_string();
                    }

                    if let Some(description) = it.next() {
                        error_condition.description = description.to_string();
                    }
                    Ok(error_condition)
                } else {
                    Err(AmqpError::decode_error(Some(
                        "Expected list with condition and description",
                    )))
                }
            }
            _ => Err(AmqpError::decode_error(Some(
                format!("Expected error descriptor but found {:?}", *descriptor).as_str(),
            ))),
        }
    } else {
        Err(AmqpError::decode_error(Some(
            "Missing expected error descriptor",
        )))
    }
}

impl End {
    pub fn from_value(value: Value) -> Result<End> {
        let mut end = End { error: None };
        if let Value::List(mut args) = value {
            if args.len() > 0 {
                let condition = decode_condition(args.remove(0))?;
                end.error = Some(condition)
            }
            Ok(end)
        } else {
            Err(AmqpError::decode_error(Some(
                "Missing expected arguments for end performative",
            )))
        }
    }
}

impl Close {
    pub fn from_value(value: Value) -> Result<Close> {
        let mut close = Close { error: None };
        if let Value::List(mut args) = value {
            if args.len() > 0 {
                let condition = decode_condition(args.remove(0))?;
                close.error = Some(condition)
            }
            Ok(close)
        } else {
            Err(AmqpError::decode_error(Some(
                "Missing expected arguments for close performative",
            )))
        }
    }
}

impl Encoder for Open {
    fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        let args = vec![
            Value::String(self.container_id.clone()),
            self.hostname.to_value(|v| Value::String(v.to_string())),
            self.max_frame_size.to_value(|v| Value::Uint(*v)),
            self.channel_max.to_value(|v| Value::Ushort(*v)),
            self.idle_timeout.to_value(|v| Value::Uint(*v)),
            self.outgoing_locales.to_value(|v| {
                Value::Array(
                    v.iter()
                        .map(|l| Value::Symbol(l.clone().into_bytes()))
                        .collect(),
                )
            }),
            self.incoming_locales.to_value(|v| {
                Value::Array(
                    v.iter()
                        .map(|l| Value::Symbol(l.clone().into_bytes()))
                        .collect(),
                )
            }),
            self.offered_capabilities.to_value(|v| {
                Value::Array(
                    v.iter()
                        .map(|c| Value::Symbol(c.clone().into_bytes()))
                        .collect(),
                )
            }),
            self.desired_capabilities.to_value(|v| {
                Value::Array(
                    v.iter()
                        .map(|c| Value::Symbol(c.clone().into_bytes()))
                        .collect(),
                )
            }),
            self.properties.to_value(|v| {
                Value::Map(BTreeMap::from_iter(
                    v.iter().map(|(k, v)| (Value::String(k.clone()), v.clone())),
                ))
            }),
        ];
        Value::Described(Box::new(DESC_OPEN), Box::new(Value::List(args))).encode(writer)
    }
}

impl Encoder for Begin {
    fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        let remote_channel = self
            .remote_channel
            .map_or_else(|| Value::Null, |c| Value::Ushort(c));
        let args = vec![
            remote_channel,
            Value::Uint(self.next_outgoing_id),
            Value::Uint(self.incoming_window),
            Value::Uint(self.outgoing_window),
            self.handle_max.to_value(|v| Value::Uint(*v)),
            self.offered_capabilities.to_value(|v| {
                Value::Array(
                    v.iter()
                        .map(|c| Value::Symbol(c.clone().into_bytes()))
                        .collect(),
                )
            }),
            self.desired_capabilities.to_value(|v| {
                Value::Array(
                    v.iter()
                        .map(|c| Value::Symbol(c.clone().into_bytes()))
                        .collect(),
                )
            }),
            self.properties.to_value(|v| {
                Value::Map(BTreeMap::from_iter(
                    v.iter().map(|(k, v)| (Value::String(k.clone()), v.clone())),
                ))
            }),
        ];
        Value::Described(Box::new(DESC_BEGIN), Box::new(Value::List(args))).encode(writer)
    }
}

impl Source {
    fn to_value(&self) -> Value {
        let args = vec![
            self.address.to_value(|v| Value::String(v.to_string())),
            Value::Uint(self.durable.unwrap_or(TerminusDurability::None) as u32),
            Value::Symbol(
                self.expiry_policy
                    .unwrap_or(TerminusExpiryPolicy::SessionEnd)
                    .to_string()
                    .into_bytes(),
            ),
            Value::Uint(self.timeout.unwrap_or(0)),
            Value::Bool(self.dynamic.unwrap_or(false)),
            self.dynamic_node_properties.to_value(|v| {
                Value::Map(BTreeMap::from_iter(
                    v.iter().map(|(k, v)| (Value::String(k.clone()), v.clone())),
                ))
            }),
            self.distribution_mode
                .to_value(|v| Value::Symbol(v.clone().into_bytes())),
            self.filter.to_value(|v| {
                Value::Map(BTreeMap::from_iter(
                    v.iter().map(|(k, v)| (Value::String(k.clone()), v.clone())),
                ))
            }),
            self.default_outcome
                .to_value(|v| Value::Symbol(v.to_string().into_bytes())),
            self.outcomes.to_value(|v| {
                Value::Array(
                    v.iter()
                        .map(|c| Value::Symbol(c.to_string().into_bytes()))
                        .collect(),
                )
            }),
            self.capabilities.to_value(|v| {
                Value::Array(
                    v.iter()
                        .map(|c| Value::Symbol(c.clone().into_bytes()))
                        .collect(),
                )
            }),
        ];
        Value::Described(Box::new(DESC_SOURCE), Box::new(Value::List(args)))
    }
}

impl Target {
    fn to_value(&self) -> Value {
        let args = vec![
            self.address.to_value(|v| Value::String(v.clone())),
            Value::Uint(self.durable.unwrap_or(TerminusDurability::None) as u32),
            Value::Symbol(
                self.expiry_policy
                    .unwrap_or(TerminusExpiryPolicy::SessionEnd)
                    .to_string()
                    .into_bytes(),
            ),
            Value::Uint(self.timeout.unwrap_or(0)),
            Value::Bool(self.dynamic.unwrap_or(false)),
            self.dynamic_node_properties.to_value(|v| {
                Value::Map(BTreeMap::from_iter(
                    v.iter().map(|(k, v)| (Value::String(k.clone()), v.clone())),
                ))
            }),
            self.capabilities.to_value(|v| {
                Value::Array(
                    v.iter()
                        .map(|c| Value::Symbol(c.clone().into_bytes()))
                        .collect(),
                )
            }),
        ];
        Value::Described(Box::new(DESC_TARGET), Box::new(Value::List(args)))
    }
}

impl Encoder for Attach {
    fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        let args = vec![
            Value::String(self.name.clone()),
            Value::Uint(self.handle),
            Value::Bool(self.role == LinkRole::Receiver),
            Value::Ubyte(self.snd_settle_mode.unwrap_or(SenderSettleMode::Mixed) as u8),
            Value::Ubyte(self.rcv_settle_mode.unwrap_or(ReceiverSettleMode::First) as u8),
            self.source.to_value(|v| v.to_value()),
            self.target.to_value(|v| v.to_value()),
            self.unsettled.to_value(|v| {
                Value::Map(BTreeMap::from_iter(
                    v.iter().map(|(k, v)| (k.clone(), v.clone())),
                ))
            }),
            Value::Bool(self.incomplete_unsettled.unwrap_or(false)),
            self.initial_delivery_count.to_value(|v| Value::Uint(*v)),
            self.max_message_size.to_value(|v| Value::Ulong(*v)),
            self.offered_capabilities.to_value(|v| {
                Value::Array(
                    v.iter()
                        .map(|c| Value::Symbol(c.clone().into_bytes()))
                        .collect(),
                )
            }),
            self.desired_capabilities.to_value(|v| {
                Value::Array(
                    v.iter()
                        .map(|c| Value::Symbol(c.clone().into_bytes()))
                        .collect(),
                )
            }),
            self.properties.to_value(|v| {
                Value::Map(BTreeMap::from_iter(
                    v.iter().map(|(k, v)| (Value::String(k.clone()), v.clone())),
                ))
            }),
        ];
        Value::Described(Box::new(DESC_ATTACH), Box::new(Value::List(args))).encode(writer)
    }
}

impl Encoder for End {
    fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        let val = self.error.to_value(|e| {
            Value::Described(
                Box::new(Value::Ulong(0x1D)),
                Box::new(Value::List(vec![
                    Value::Symbol(e.condition.clone().into_bytes()),
                    Value::String(e.description.clone()),
                ])),
            )
        });
        Value::Described(Box::new(DESC_CLOSE), Box::new(Value::List(vec![val]))).encode(writer)
    }
}

impl Encoder for Close {
    fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        let val = self.error.to_value(|e| {
            Value::Described(
                Box::new(Value::Ulong(0x1D)),
                Box::new(Value::List(vec![
                    Value::Symbol(e.condition.clone().into_bytes()),
                    Value::String(e.description.clone()),
                ])),
            )
        });
        Value::Described(Box::new(DESC_CLOSE), Box::new(Value::List(vec![val]))).encode(writer)
    }
}

impl FrameHeader {
    pub fn decode(reader: &mut dyn Read) -> Result<FrameHeader> {
        Ok(FrameHeader {
            size: reader.read_u32::<NetworkEndian>()?,
            doff: reader.read_u8()?,
            frame_type: reader.read_u8()?,
            ext: reader.read_u16::<NetworkEndian>()?,
        })
    }

    pub fn encode(self: &Self, writer: &mut dyn Write) -> Result<()> {
        writer.write_u32::<NetworkEndian>(self.size)?;
        writer.write_u8(self.doff)?;
        writer.write_u8(self.frame_type)?;
        writer.write_u16::<NetworkEndian>(self.ext)?;
        Ok(())
    }
}

impl Frame {
    pub fn encode(self: &Self, writer: &mut dyn Write) -> Result<usize> {
        let mut header: FrameHeader = FrameHeader {
            size: 8,
            doff: 2,
            frame_type: 0,
            ext: 0,
        };

        let mut buf: Vec<u8> = Vec::new();

        match self {
            Frame::AMQP(AmqpFrame { channel, body }) => {
                header.frame_type = 0;
                header.ext = *channel;

                if let Some(body) = body {
                    match body {
                        Performative::Open(open) => {
                            open.encode(&mut buf)?;
                        }
                        Performative::Begin(begin) => {
                            begin.encode(&mut buf)?;
                        }
                        Performative::Attach(attach) => {
                            attach.encode(&mut buf)?;
                        }
                        Performative::End(end) => {
                            end.encode(&mut buf)?;
                        }
                        Performative::Close(close) => {
                            close.encode(&mut buf)?;
                        }
                        _ => {
                            println!("Unable to encode frame {:?}", self);
                            return Err(AmqpError::not_implemented());
                        }
                    }
                }
            }
            Frame::SASL(sasl_frame) => {
                header.frame_type = 1;
                match sasl_frame {
                    SaslFrame::SaslMechanisms(_) => {}
                    SaslFrame::SaslInit(init) => {
                        init.encode(&mut buf)?;
                    }
                    SaslFrame::SaslChallenge(_) => {}
                    SaslFrame::SaslResponse(_) => {}
                    SaslFrame::SaslOutcome(_) => {}
                }
            }
        }

        header.size += buf.len() as u32;

        header.encode(writer)?;
        writer.write_all(&buf[..])?;

        Ok(header.size as usize)
    }

    pub fn decode(header: FrameHeader, reader: &mut dyn Read) -> Result<Frame> {
        // Read off extended header not in use
        let mut doff = header.doff;
        while doff > 2 {
            reader.read_u32::<NetworkEndian>()?;
            doff -= 1;
        }

        if header.frame_type == 0 {
            let body = if header.size > 8 {
                if let Value::Described(descriptor, value) = decode_value(reader)? {
                    Some(match *descriptor {
                        DESC_OPEN => {
                            let open = Open::from_value(*value)?;
                            Ok(Performative::Open(open))
                        }
                        DESC_CLOSE => {
                            let close = Close::from_value(*value)?;
                            Ok(Performative::Close(close))
                        }
                        DESC_BEGIN => {
                            let begin = Begin::from_value(*value)?;
                            Ok(Performative::Begin(begin))
                        }
                        DESC_END => {
                            let end = End::from_value(*value)?;
                            Ok(Performative::End(end))
                        }
                        v => Err(AmqpError::amqp_error(
                            condition::DECODE_ERROR,
                            Some(format!("Unexpected descriptor value: {:?}", v).as_str()),
                        )),
                    }?)
                } else {
                    None
                }
            } else {
                None
            };
            Ok(Frame::AMQP(AmqpFrame {
                channel: header.ext,
                body: body,
            }))
        } else if header.frame_type == 1 {
            if header.size > 8 {
                if let Value::Described(descriptor, value) = decode_value(reader)? {
                    let frame = match *descriptor {
                        DESC_SASL_MECHANISMS => {
                            if let Value::List(args) = *value {
                                let mut it = args.iter();

                                if let Some(sasl_server_mechanisms) = it.next() {
                                    if let Value::Array(vec) = sasl_server_mechanisms {
                                        let mut mechs = Vec::new();
                                        for val in vec.iter() {
                                            mechs.push(val.to_string().parse::<SaslMechanism>()?)
                                        }
                                        Some(SaslFrame::SaslMechanisms(mechs))
                                    } else if let Value::Symbol(s) = sasl_server_mechanisms {
                                        Some(SaslFrame::SaslMechanisms(vec![String::from_utf8(
                                            s.to_vec(),
                                        )
                                        .unwrap()
                                        .parse::<SaslMechanism>()?]))
                                    } else {
                                        Some(SaslFrame::SaslMechanisms(vec![]))
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }
                        DESC_SASL_OUTCOME => {
                            if let Value::List(args) = *value {
                                let mut it = args.iter();
                                let mut outcome = SaslOutcome {
                                    code: 4,
                                    additional_data: None,
                                };

                                if let Some(Value::Ubyte(code)) = it.next() {
                                    outcome.code = *code;
                                }

                                if let Some(Value::Binary(additional_data)) = it.next() {
                                    outcome.additional_data = Some(additional_data.to_vec());
                                }
                                Some(SaslFrame::SaslOutcome(outcome))
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };

                    match frame {
                        Some(frame) => Ok(Frame::SASL(frame)),
                        None => Err(AmqpError::decode_error(Some("Error decoding sasl frame"))),
                    }
                } else {
                    Err(AmqpError::amqp_error(
                        condition::connection::FRAMING_ERROR,
                        Some("Sasl frame not matched"),
                    ))
                }
            } else {
                Err(AmqpError::amqp_error(
                    condition::connection::FRAMING_ERROR,
                    Some("Sasl frame not matched"),
                ))
            }
        } else {
            Err(AmqpError::amqp_error(
                condition::connection::FRAMING_ERROR,
                Some(format!("Unknown frame type {}", header.frame_type).as_str()),
            ))
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::error::*;
    use crate::types::*;

    #[test]
    fn check_performatives() {
        let frm = Open::new("1234");

        assert_eq!(None, frm.hostname);
        assert_eq!(4, frm.container_id.len());
        assert_eq!(None, frm.max_frame_size);
        assert_eq!(None, frm.channel_max);
    }
}
