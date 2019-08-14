/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use byteorder::NetworkEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use std::collections::BTreeMap;
use std::convert::From;
use std::io::Read;
use std::io::Write;
use std::iter::FromIterator;
use std::vec::Vec;
use uuid::Uuid;

use crate::error::*;
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
    AMQP {
        channel: u16,
        body: Option<Performative>,
    },
    SASL,
}

#[derive(Debug)]
pub enum Performative {
    Open(Open),
    Close(Close),
    Begin(Begin),
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
pub struct End {
    pub error: Option<ErrorCondition>,
}

#[derive(Debug, Clone)]
pub struct Close {
    pub error: Option<ErrorCondition>,
}

const DESC_OPEN: Value = Value::Ulong(0x10);
const DESC_BEGIN: Value = Value::Ulong(0x11);
const DESC_END: Value = Value::Ulong(0x17);
const DESC_CLOSE: Value = Value::Ulong(0x18);

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
}

impl ToValue for Open {
    fn to_value(&self) -> Value {
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
        Value::Described(Box::new(DESC_OPEN), Box::new(Value::List(args)))
    }
}

impl ToValue for Begin {
    fn to_value(&self) -> Value {
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
        Value::Described(Box::new(DESC_BEGIN), Box::new(Value::List(args)))
    }
}

impl ToValue for End {
    fn to_value(&self) -> Value {
        let val = self.error.to_value(|e| {
            Value::Described(
                Box::new(Value::Ulong(0x1D)),
                Box::new(Value::List(vec![
                    Value::Symbol(e.condition.clone().into_bytes()),
                    Value::String(e.description.clone()),
                ])),
            )
        });
        Value::Described(Box::new(DESC_CLOSE), Box::new(Value::List(vec![val])))
    }
}

impl ToValue for Close {
    fn to_value(&self) -> Value {
        let val = self.error.to_value(|e| {
            Value::Described(
                Box::new(Value::Ulong(0x1D)),
                Box::new(Value::List(vec![
                    Value::Symbol(e.condition.clone().into_bytes()),
                    Value::String(e.description.clone()),
                ])),
            )
        });
        Value::Described(Box::new(DESC_CLOSE), Box::new(Value::List(vec![val])))
    }
}

pub fn encode_frame(frame: &Frame, writer: &mut Write) -> Result<usize> {
    let mut header: FrameHeader = FrameHeader {
        size: 8,
        doff: 2,
        frame_type: 0,
        ext: 0,
    };
    match frame {
        Frame::AMQP { channel, body } => {
            header.frame_type = 0;
            header.ext = *channel;

            let mut buf: Vec<u8> = Vec::new();

            if let Some(body) = body {
                match body {
                    Performative::Open(open) => {
                        encode_value(&open.to_value(), &mut buf)?;
                    }
                    Performative::Begin(begin) => {
                        encode_value(&begin.to_value(), &mut buf)?;
                    }
                    Performative::End(end) => {
                        encode_value(&end.to_value(), &mut buf)?;
                    }
                    Performative::Close(close) => {
                        encode_value(&close.to_value(), &mut buf)?;
                    }
                }
            }
            header.size += buf.len() as u32;

            encode_header(&header, writer)?;
            writer.write_all(&buf[..]);

            Ok(header.size as usize)
        }
        Frame::SASL => Err(AmqpError::amqp_error(condition::NOT_IMPLEMENTED, None)),
    }
}

pub fn decode_header(reader: &mut Read) -> Result<FrameHeader> {
    Ok(FrameHeader {
        size: reader.read_u32::<NetworkEndian>()?,
        doff: reader.read_u8()?,
        frame_type: reader.read_u8()?,
        ext: reader.read_u16::<NetworkEndian>()?,
    })
}

pub fn encode_header(header: &FrameHeader, writer: &mut Write) -> Result<()> {
    writer.write_u32::<NetworkEndian>(header.size)?;
    writer.write_u8(header.doff)?;
    writer.write_u8(header.frame_type)?;
    writer.write_u16::<NetworkEndian>(header.ext)?;
    Ok(())
}

pub fn decode_frame(header: FrameHeader, stream: &mut Read) -> Result<Frame> {
    if header.frame_type == 0 {
        let body = if header.size > 8 {
            let mut doff = header.doff;
            while doff > 2 {
                stream.read_u32::<NetworkEndian>()?;
                doff -= 1;
            }
            if let Value::Described(descriptor, value) = decode_value(stream)? {
                Some(match *descriptor {
                    DESC_OPEN => {
                        if let Value::List(args) = *value {
                            let mut it = args.iter();

                            let container_id = it.next().and_then(|c| c.try_to_string()).ok_or(
                                AmqpError::decode_error(Some(
                                    "Unable to decode mandatory field 'container_id'",
                                )),
                            )?;

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
                                println!("OLOC {:?}", outgoing_locales);
                            }

                            if let Some(incoming_locales) = it.next() {
                                // TODO:
                                println!("ILOC {:?}", incoming_locales);
                            }

                            if let Some(offered_capabilities) = it.next() {
                                if let Value::Array(vec) = offered_capabilities {
                                    let mut cap = Vec::new();
                                    for val in vec.iter() {
                                        cap.push(val.to_string())
                                    }
                                    open.offered_capabilities = Some(cap);
                                } else if let Value::Symbol(s) = offered_capabilities {
                                    open.offered_capabilities =
                                        Some(vec![String::from_utf8(s.to_vec()).unwrap()]);
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
                                    open.desired_capabilities =
                                        Some(vec![String::from_utf8(s.to_vec()).unwrap()]);
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

                            Ok(Performative::Open(open))
                        } else {
                            Err(AmqpError::amqp_error(
                                condition::DECODE_ERROR,
                                Some("Missing expected arguments for open performative"),
                            ))
                        }
                    }
                    Value::Ulong(0x18) => {
                        let mut close = Close { error: None };
                        if let Value::List(args) = *value {
                            if args.len() > 0 {
                                if let Value::Ulong(0x1D) = args[0] {
                                    let list = decode_value(stream)?;
                                    if let Value::List(args) = list {
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
                                        close.error = Some(error_condition);
                                    }
                                }
                            }
                        }
                        Ok(Performative::Close(close))
                    }
                    Value::Ulong(0x11) => {
                        if let Value::List(args) = *value {
                            let mut begin = Begin::new(0, 0, 0);
                            let mut it = args.iter();
                            if let Some(remote_channel) = it.next() {
                                begin.remote_channel = remote_channel.try_to_u16();
                            }

                            begin.next_outgoing_id = it.next().and_then(|c| c.try_to_u32()).ok_or(
                                AmqpError::decode_error(Some(
                                    "Unable to decode mandatory field 'next-outgoing-id'",
                                )),
                            )?;

                            begin.incoming_window = it.next().and_then(|c| c.try_to_u32()).ok_or(
                                AmqpError::decode_error(Some(
                                    "Unable to decode mandatory field 'incoming-window'",
                                )),
                            )?;

                            begin.outgoing_window = it.next().and_then(|c| c.try_to_u32()).ok_or(
                                AmqpError::decode_error(Some(
                                    "Unable to decode mandatory field 'outgoing-window'",
                                )),
                            )?;

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
                                    begin.offered_capabilities =
                                        Some(vec![String::from_utf8(s.to_vec()).unwrap()]);
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
                                    begin.desired_capabilities =
                                        Some(vec![String::from_utf8(s.to_vec()).unwrap()]);
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

                            Ok(Performative::Begin(begin))
                        } else {
                            Err(AmqpError::amqp_error(
                                condition::DECODE_ERROR,
                                Some("Missing expected arguments for begin performative"),
                            ))
                        }
                    }
                    Value::Ulong(0x17) => {
                        let mut end = End { error: None };
                        if let Value::List(args) = *value {
                            if args.len() > 0 {
                                if let Value::Ulong(0x1D) = args[0] {
                                    let list = decode_value(stream)?;
                                    if let Value::List(args) = list {
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
                                        end.error = Some(error_condition);
                                    }
                                }
                            }
                        }
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
        Ok(Frame::AMQP {
            channel: header.ext,
            body: body,
        })
    //} else if frame_type == 1 {
    // SASL
    } else {
        Err(AmqpError::amqp_error(
            condition::connection::FRAMING_ERROR,
            Some(format!("Unknown frame type {}", header.frame_type).as_str()),
        ))
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
