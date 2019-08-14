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

#[derive(Debug, Clone)]
pub struct Open {
    pub container_id: String,
    pub hostname: String,
    pub max_frame_size: u32,
    pub channel_max: u16,
    pub idle_timeout: u32,
    pub outgoing_locales: Vec<String>,
    pub incoming_locales: Vec<String>,
    pub offered_capabilities: Vec<String>,
    pub desired_capabilities: Vec<String>,
    pub properties: BTreeMap<String, Value>,
}

impl Open {
    pub fn new(container_id: &str) -> Open {
        Open {
            container_id: container_id.to_string(),
            hostname: String::new(),
            max_frame_size: 4294967295,
            channel_max: 65535,
            idle_timeout: 0,
            outgoing_locales: Vec::new(),
            incoming_locales: Vec::new(),
            offered_capabilities: Vec::new(),
            desired_capabilities: Vec::new(),
            properties: BTreeMap::new(),
        }
    }
}

impl ToValue for Open {
    fn to_value(&self) -> Value {
        let args = vec![
            Value::String(self.container_id.clone()),
            Value::String(self.hostname.clone()),
            Value::Uint(self.max_frame_size),
            Value::Ushort(self.channel_max),
            Value::Uint(self.idle_timeout),
            Value::Array(
                self.outgoing_locales
                    .iter()
                    .map(|l| Value::Symbol(l.clone().into_bytes()))
                    .collect(),
            ),
            Value::Array(
                self.incoming_locales
                    .iter()
                    .map(|l| Value::Symbol(l.clone().into_bytes()))
                    .collect(),
            ),
            Value::Array(
                self.offered_capabilities
                    .iter()
                    .map(|c| Value::Symbol(c.clone().into_bytes()))
                    .collect(),
            ),
            Value::Array(
                self.desired_capabilities
                    .iter()
                    .map(|c| Value::Symbol(c.clone().into_bytes()))
                    .collect(),
            ),
            Value::Map(BTreeMap::from_iter(
                self.properties
                    .iter()
                    .map(|(k, v)| (Value::String(k.clone()), v.clone())),
            )),
        ];
        Value::Described(
            Box::new(Value::Ulong(PerformativeCode::Open as u64)),
            Box::new(Value::List(args)),
        )
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
            Value::Uint(self.handle_max),
            Value::Array(
                self.offered_capabilities
                    .iter()
                    .map(|c| Value::Symbol(c.clone().into_bytes()))
                    .collect(),
            ),
            Value::Array(
                self.desired_capabilities
                    .iter()
                    .map(|c| Value::Symbol(c.clone().into_bytes()))
                    .collect(),
            ),
            Value::Map(BTreeMap::from_iter(
                self.properties
                    .iter()
                    .map(|(k, v)| (Value::String(k.clone()), v.clone())),
            )),
        ];
        Value::Described(
            Box::new(Value::Ulong(PerformativeCode::Begin as u64)),
            Box::new(Value::List(args)),
        )
    }
}

impl ToValue for End {
    fn to_value(&self) -> Value {
        let args = if self.error.is_none() {
            vec![Value::Null]
        } else {
            let e = self.error.as_ref().unwrap();
            vec![
                Value::Ulong(0x1D),
                Value::List(vec![
                    Value::Symbol(e.condition.clone().into_bytes()),
                    Value::String(e.description.clone()),
                ]),
            ]
        };
        Value::Described(
            Box::new(Value::Ulong(PerformativeCode::End as u64)),
            Box::new(Value::List(args)),
        )
    }
}

impl ToValue for Close {
    fn to_value(&self) -> Value {
        let args = if self.error.is_none() {
            vec![Value::Null]
        } else {
            let e = self.error.as_ref().unwrap();
            vec![
                Value::Ulong(0x1D),
                Value::List(vec![
                    Value::Symbol(e.condition.clone().into_bytes()),
                    Value::String(e.description.clone()),
                ]),
            ]
        };
        Value::Described(
            Box::new(Value::Ulong(PerformativeCode::Close as u64)),
            Box::new(Value::List(args)),
        )
    }
}

#[derive(Debug, Clone)]
pub struct Close {
    pub error: Option<ErrorCondition>,
}

#[derive(Debug, Clone)]
pub struct Begin {
    pub remote_channel: Option<u16>,
    pub next_outgoing_id: u32,
    pub incoming_window: u32,
    pub outgoing_window: u32,
    pub handle_max: u32,
    pub offered_capabilities: Vec<String>,
    pub desired_capabilities: Vec<String>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct End {
    pub error: Option<ErrorCondition>,
}

#[derive(Debug)]
pub enum Performative {
    Open(Open),
    Close(Close),
    Begin(Begin),
    End(End),
}

#[repr(u64)]
#[derive(Clone, PartialEq, Debug, PartialOrd)]
pub enum PerformativeCode {
    Open = 0x10,
    Begin = 0x11,
    End = 0x17,
    Close = 0x18,
}

#[derive(Debug)]
pub enum Frame {
    AMQP {
        channel: u16,
        body: Option<Performative>,
    },
    SASL,
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

#[derive(Debug)]
pub struct FrameHeader {
    pub size: u32,
    doff: u8,
    frame_type: u8,
    ext: u16,
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
                    Value::Ulong(0x10) => {
                        if let Value::List(args) = *value {
                            let mut it = args.iter();

                            let container_id = it.next().and_then(|c| c.try_to_string()).ok_or(
                                AmqpError::decode_error(Some(
                                    "Unable to decode mandatory field 'container_id'",
                                )),
                            )?;

                            let mut open = Open::new(container_id.as_str());
                            if let Some(hostname) = it.next() {
                                open.hostname =
                                    hostname.try_to_string().unwrap_or_else(|| String::new());
                            }

                            if let Some(max_frame_size) = it.next() {
                                open.max_frame_size = max_frame_size.to_u32();
                            }

                            if let Some(channel_max) = it.next() {
                                open.channel_max = channel_max.to_u16();
                            }

                            if let Some(idle_timeout) = it.next() {
                                open.idle_timeout = idle_timeout.to_u32();
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
                                    for val in vec.iter() {
                                        open.offered_capabilities.push(val.to_string())
                                    }
                                } else {
                                    offered_capabilities
                                        .try_to_string()
                                        .map(|s| open.offered_capabilities.push(s));
                                }
                            }

                            if let Some(desired_capabilities) = it.next() {
                                if let Value::Array(vec) = desired_capabilities {
                                    for val in vec.iter() {
                                        open.desired_capabilities.push(val.to_string())
                                    }
                                } else {
                                    desired_capabilities
                                        .try_to_string()
                                        .map(|s| open.desired_capabilities.push(s));
                                }
                            }

                            if let Some(properties) = it.next() {
                                if let Value::Map(m) = properties {
                                    for (key, value) in m.iter() {
                                        open.properties.insert(key.to_string(), value.clone());
                                    }
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
                            let mut begin = Begin {
                                remote_channel: None,
                                next_outgoing_id: 0,
                                incoming_window: 0,
                                outgoing_window: 0,
                                handle_max: std::u32::MAX,
                                offered_capabilities: Vec::new(),
                                desired_capabilities: Vec::new(),
                                properties: BTreeMap::new(),
                            };
                            let mut it = args.iter();
                            if let Some(remote_channel) = it.next() {
                                begin.remote_channel = remote_channel.try_to_u16();
                            }

                            if let Some(next_outgoing_id) = it.next() {
                                begin.next_outgoing_id = next_outgoing_id.to_u32();
                            }

                            if let Some(incoming_window) = it.next() {
                                begin.incoming_window = incoming_window.to_u32();
                            }

                            if let Some(outgoing_window) = it.next() {
                                begin.outgoing_window = outgoing_window.to_u32();
                            }

                            if let Some(handle_max) = it.next() {
                                begin.handle_max = handle_max.to_u32();
                            }

                            if let Some(offered_capabilities) = it.next() {
                                if let Value::Array(vec) = offered_capabilities {
                                    for val in vec.iter() {
                                        begin.offered_capabilities.push(val.to_string())
                                    }
                                } else {
                                    offered_capabilities
                                        .try_to_string()
                                        .map(|s| begin.offered_capabilities.push(s));
                                }
                            }

                            if let Some(desired_capabilities) = it.next() {
                                if let Value::Array(vec) = desired_capabilities {
                                    for val in vec.iter() {
                                        begin.desired_capabilities.push(val.to_string())
                                    }
                                } else {
                                    desired_capabilities
                                        .try_to_string()
                                        .map(|s| begin.desired_capabilities.push(s));
                                }
                            }

                            if let Some(properties) = it.next() {
                                if let Value::Map(m) = properties {
                                    for (key, value) in m.iter() {
                                        begin.properties.insert(key.to_string(), value.clone());
                                    }
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

        assert_eq!(String::from(""), frm.hostname);
        assert_eq!(4, frm.container_id.len());
        assert_eq!(4294967295, frm.max_frame_size);
        assert_eq!(65535, frm.channel_max);
    }
}
