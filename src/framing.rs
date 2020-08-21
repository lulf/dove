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
use std::vec::Vec;

use crate::error::*;
use crate::sasl::*;
use crate::types::*;

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

#[derive(Debug)]
pub struct SaslMechanisms {
    pub mechanisms: Vec<SaslMechanism>,
}

#[derive(Debug)]
pub struct SaslInit {
    pub mechanism: SaslMechanism,
    pub initial_response: Option<Vec<u8>>,
    pub hostname: Option<String>,
}

pub type SaslChallenge = Vec<u8>;
pub type SaslResponse = Vec<u8>;

#[derive(Debug)]
pub struct SaslOutcome {
    pub code: SaslCode,
    pub additional_data: Option<Vec<u8>>,
}

pub type SaslCode = u8;

impl Encoder for SaslMechanism {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let value = match self {
            SaslMechanism::Anonymous => Symbol::from_slice(b"ANONYMOUS"),
            SaslMechanism::Plain => Symbol::from_slice(b"PLAIN"),
            SaslMechanism::CramMd5 => Symbol::from_slice(b"CRAM-MD5"),
            SaslMechanism::ScramSha1 => Symbol::from_slice(b"SCRAM-SHA-1"),
            SaslMechanism::ScramSha256 => Symbol::from_slice(b"SCRAM-SHA-256"),
        };
        value.encode(writer)
    }
}

impl Encoder for SaslInit {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_SASL_INIT);
        encoder.encode_arg(&self.mechanism)?;
        encoder.encode_arg(&self.initial_response)?;
        encoder.encode_arg(&self.hostname)?;
        encoder.encode(writer)
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
    pub outgoing_locales: Option<Vec<Symbol>>,
    pub incoming_locales: Option<Vec<Symbol>>,
    pub offered_capabilities: Option<Vec<Symbol>>,
    pub desired_capabilities: Option<Vec<Symbol>>,
    pub properties: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone)]
pub struct Begin {
    pub remote_channel: Option<u16>,
    pub next_outgoing_id: u32,
    pub incoming_window: u32,
    pub outgoing_window: u32,
    pub handle_max: Option<u32>,
    pub offered_capabilities: Option<Vec<Symbol>>,
    pub desired_capabilities: Option<Vec<Symbol>>,
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
    pub offered_capabilities: Option<Vec<Symbol>>,
    pub desired_capabilities: Option<Vec<Symbol>>,
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
    pub distribution_mode: Option<Symbol>,
    pub filter: Option<BTreeMap<String, Value>>,
    pub default_outcome: Option<Outcome>,
    pub outcomes: Option<Vec<Outcome>>,
    pub capabilities: Option<Vec<Symbol>>,
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

    pub fn decode(mut decoder: FrameDecoder) -> Result<Open> {
        let mut open = Open {
            container_id: String::new(),
            hostname: None,
            max_frame_size: None,
            channel_max: None,
            idle_timeout: None,
            outgoing_locales: None,
            incoming_locales: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };
        decoder.decode_required(&mut open.container_id)?;
        decoder.decode_optional(&mut open.hostname)?;
        decoder.decode_optional(&mut open.max_frame_size)?;
        decoder.decode_optional(&mut open.channel_max)?;
        decoder.decode_optional(&mut open.idle_timeout)?;
        decoder.decode_optional(&mut open.outgoing_locales)?;
        decoder.decode_optional(&mut open.incoming_locales)?;
        decoder.decode_optional(&mut open.offered_capabilities)?;
        decoder.decode_optional(&mut open.desired_capabilities)?;
        decoder.decode_optional(&mut open.properties)?;
        Ok(open)
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

    pub fn decode(mut decoder: FrameDecoder) -> Result<Begin> {
        let mut begin = Begin::new(0, 0, 0);
        decoder.decode_optional(&mut begin.remote_channel)?;
        decoder.decode_required(&mut begin.next_outgoing_id)?;
        decoder.decode_required(&mut begin.incoming_window)?;
        decoder.decode_required(&mut begin.outgoing_window)?;
        decoder.decode_optional(&mut begin.handle_max)?;
        decoder.decode_optional(&mut begin.offered_capabilities)?;
        decoder.decode_optional(&mut begin.desired_capabilities)?;
        decoder.decode_optional(&mut begin.properties)?;
        return Ok(begin);
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

impl End {
    pub fn decode(mut decoder: FrameDecoder) -> Result<End> {
        let mut end = End { error: None };
        decoder.decode_optional(&mut end.error)?;
        Ok(end)
    }
}

impl Close {
    pub fn decode(mut decoder: FrameDecoder) -> Result<Close> {
        let mut close = Close { error: None };
        decoder.decode_optional(&mut close.error)?;
        Ok(close)
    }
}

impl SaslOutcome {
    pub fn decode(mut decoder: FrameDecoder) -> Result<SaslOutcome> {
        let mut outcome = SaslOutcome {
            code: 4,
            additional_data: None,
        };
        decoder.decode_required(&mut outcome.code)?;
        decoder.decode_optional(&mut outcome.additional_data)?;
        Ok(outcome)
    }
}

impl SaslMechanisms {
    pub fn decode(mut decoder: FrameDecoder) -> Result<SaslMechanisms> {
        let mut mechs = SaslMechanisms {
            mechanisms: Vec::new(),
        };
        decoder.decode_optional(&mut mechs.mechanisms)?;
        Ok(mechs)
    }
}

impl std::convert::TryFrom<Value> for SaslMechanism {
    type Error = AmqpError;
    fn try_from(value: Value) -> Result<Self> {
        match value {
            // TODO
            Value::Symbol(_) => return Ok(SaslMechanism::Anonymous),
            _ => Err(AmqpError::amqp_error(
                condition::DECODE_ERROR,
                Some("Error converting value to SaslMechanism"),
            )),
        }
    }
}

impl std::convert::TryFrom<Value> for Vec<SaslMechanism> {
    type Error = AmqpError;
    fn try_from(value: Value) -> Result<Self> {
        match value {
            // TODO
            Value::Array(_) => return Ok(vec![SaslMechanism::Anonymous]),
            Value::Symbol(_) => return Ok(vec![SaslMechanism::Anonymous]),
            _ => Err(AmqpError::amqp_error(
                condition::DECODE_ERROR,
                Some("Error converting value to SaslMechanism"),
            )),
        }
    }
}

impl Encoder for Open {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_OPEN);
        encoder.encode_arg(&self.container_id)?;
        encoder.encode_arg(&self.hostname)?;
        encoder.encode_arg(&self.max_frame_size)?;
        encoder.encode_arg(&self.channel_max)?;
        encoder.encode_arg(&self.idle_timeout)?;
        encoder.encode_arg(&self.outgoing_locales)?;
        encoder.encode_arg(&self.incoming_locales)?;
        encoder.encode_arg(&self.offered_capabilities)?;
        encoder.encode_arg(&self.desired_capabilities)?;
        encoder.encode_arg(&self.properties)?;
        encoder.encode(writer)
    }
}

impl Encoder for Begin {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_BEGIN);
        encoder.encode_arg(&self.remote_channel)?;
        encoder.encode_arg(&self.next_outgoing_id)?;
        encoder.encode_arg(&self.incoming_window)?;
        encoder.encode_arg(&self.outgoing_window)?;
        encoder.encode_arg(&self.handle_max)?;
        encoder.encode_arg(&self.offered_capabilities)?;
        encoder.encode_arg(&self.desired_capabilities)?;
        encoder.encode_arg(&self.properties)?;
        encoder.encode(writer)
    }
}

impl Encoder for Source {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_SOURCE);
        encoder.encode_arg(&self.address)?;
        encoder.encode_arg(&self.durable.unwrap_or(TerminusDurability::None))?;
        encoder.encode_arg(
            &self
                .expiry_policy
                .unwrap_or(TerminusExpiryPolicy::SessionEnd),
        )?;
        encoder.encode_arg(&self.timeout.unwrap_or(0))?;
        encoder.encode_arg(&self.dynamic.unwrap_or(false))?;
        encoder.encode_arg(&self.dynamic_node_properties)?;
        encoder.encode_arg(&self.distribution_mode)?;
        encoder.encode_arg(&self.filter)?;
        encoder.encode_arg(&self.default_outcome)?;
        encoder.encode_arg(&self.outcomes)?;
        encoder.encode_arg(&self.capabilities)?;
        encoder.encode(writer)
    }
}

impl Encoder for Target {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_TARGET);
        encoder.encode_arg(&self.address)?;
        encoder.encode_arg(&self.durable.unwrap_or(TerminusDurability::None))?;
        encoder.encode_arg(
            &self
                .expiry_policy
                .unwrap_or(TerminusExpiryPolicy::SessionEnd),
        )?;
        encoder.encode_arg(&self.timeout.unwrap_or(0))?;
        encoder.encode_arg(&self.dynamic.unwrap_or(false))?;
        encoder.encode_arg(&self.dynamic_node_properties)?;
        encoder.encode_arg(&self.capabilities)?;
        encoder.encode(writer)
    }
}

impl Encoder for Attach {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_ATTACH);
        encoder.encode_arg(&self.name)?;
        encoder.encode_arg(&self.handle)?;
        encoder.encode_arg(&self.role)?;
        encoder.encode_arg(&self.snd_settle_mode.unwrap_or(SenderSettleMode::Mixed))?;
        encoder.encode_arg(&self.rcv_settle_mode.unwrap_or(ReceiverSettleMode::First))?;
        encoder.encode_arg(&self.source)?;
        encoder.encode_arg(&self.target)?;
        encoder.encode_arg(&self.unsettled)?;
        encoder.encode_arg(&self.incomplete_unsettled.unwrap_or(false))?;
        encoder.encode_arg(&self.initial_delivery_count)?;
        encoder.encode_arg(&self.max_message_size)?;
        encoder.encode_arg(&self.max_message_size)?;
        encoder.encode_arg(&self.offered_capabilities)?;
        encoder.encode_arg(&self.desired_capabilities)?;
        encoder.encode_arg(&self.properties)?;
        encoder.encode(writer)
    }
}

impl Encoder for End {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_END);
        encoder.encode_arg(&self.error)?;
        encoder.encode(writer)
        /*
        let val = self.error.to_value(|e| {
            Value::Described(
                Box::new(Value::Ulong(0x1D)),
                Box::new(Value::List(vec![
                    Value::Symbol(e.condition.clone().into_bytes()),
                    Value::String(e.description.clone()),
                ])),
            )
        });
            */
    }
}

impl Encoder for Close {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_CLOSE);
        encoder.encode_arg(&self.error)?;
        encoder.encode(writer)
    }
}

impl Encoder for SenderSettleMode {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::Ubyte(&(*self as u8)).encode(writer)
    }
}

impl Encoder for ReceiverSettleMode {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        Value::Ubyte(*self as u8).encode(writer)
    }
}

impl Encoder for LinkRole {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        Value::Bool(*self == LinkRole::Receiver).encode(writer)
    }
}

impl Encoder for Vec<Outcome> {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut values = Vec::new();
        for outcome in self.iter() {
            values.push(ValueRef::SymbolRef(outcome.to_str()));
        }
        ValueRef::ArrayRef(&values).encode(writer)
    }
}

impl Outcome {
    pub fn to_str(&self) -> &'static str {
        match *self {
            Outcome::Accepted => "Accepted",
            Outcome::Rejected => "Rejected",
            Outcome::Released => "Released",
            Outcome::Modified => "Modified",
        }
    }
}
impl Encoder for Outcome {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::SymbolRef(self.to_str()).encode(writer)
    }
}

impl TerminusDurability {
    pub fn to_str(&self) -> &'static str {
        match *self {
            TerminusDurability::None => "None",
            TerminusDurability::Configuration => "Configuration",
            TerminusDurability::UnsettledState => "UnsettledState",
        }
    }
}

impl TerminusExpiryPolicy {
    pub fn to_str(&self) -> &'static str {
        match *self {
            TerminusExpiryPolicy::LinkDetach => "LinkDetach",
            TerminusExpiryPolicy::SessionEnd => "SessionEnd",
            TerminusExpiryPolicy::ConnectionClose => "ConnectionClose",
            TerminusExpiryPolicy::Never => "Never",
        }
    }
}

impl Encoder for TerminusDurability {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::SymbolRef(self.to_str()).encode(writer)
    }
}

impl Encoder for TerminusExpiryPolicy {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::SymbolRef(self.to_str()).encode(writer)
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
                if let Value::Described(descriptor, mut value) = decode_value(reader)? {
                    let decoder = FrameDecoder::new(&descriptor, &mut value)?;
                    Some(match *descriptor {
                        DESC_OPEN => {
                            let open = Open::decode(decoder)?;
                            Ok(Performative::Open(open))
                        }
                        DESC_CLOSE => {
                            let close = Close::decode(decoder)?;
                            Ok(Performative::Close(close))
                        }
                        DESC_BEGIN => {
                            let begin = Begin::decode(decoder)?;
                            Ok(Performative::Begin(begin))
                        }
                        DESC_END => {
                            let end = End::decode(decoder)?;
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
                if let Value::Described(descriptor, mut value) = decode_value(reader)? {
                    let decoder = FrameDecoder::new(&descriptor, &mut value)?;
                    let frame = match *descriptor {
                        DESC_SASL_MECHANISMS => {
                            Some(SaslFrame::SaslMechanisms(SaslMechanisms::decode(decoder)?))
                        }
                        DESC_SASL_OUTCOME => {
                            Some(SaslFrame::SaslOutcome(SaslOutcome::decode(decoder)?))
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

    #[test]
    fn check_performatives() {
        let frm = Open::new("1234");

        assert_eq!(None, frm.hostname);
        assert_eq!(4, frm.container_id.len());
        assert_eq!(None, frm.max_frame_size);
        assert_eq!(None, frm.channel_max);
    }
}
