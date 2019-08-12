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

impl Default for Open {
    fn default() -> Open {
        Open {
            container_id: Uuid::new_v4().to_string(),
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

#[derive(Debug)]
pub enum Frame {
    AMQP {
        channel: u16,
        performative: Option<Performative>,
        payload: Option<Box<Vec<u8>>>,
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
        Frame::AMQP {
            channel,
            performative,
            payload,
        } => {
            header.frame_type = 0;
            header.ext = *channel;

            let mut body: Vec<u8> = Vec::new();

            if let Some(performative) = performative {
                match performative {
                    Performative::Open(open) => {
                        body.write_u8(0)?;
                        encode_ref(&Value::Ulong(0x10), &mut body)?;
                        let args = vec![
                            Value::String(open.container_id.clone()),
                            Value::String(open.hostname.clone()),
                            Value::Uint(open.max_frame_size),
                            Value::Ushort(open.channel_max),
                            Value::Uint(open.idle_timeout),
                            Value::Array(
                                open.outgoing_locales
                                    .iter()
                                    .map(|l| Value::Symbol(l.clone().into_bytes()))
                                    .collect(),
                            ),
                            Value::Array(
                                open.incoming_locales
                                    .iter()
                                    .map(|l| Value::Symbol(l.clone().into_bytes()))
                                    .collect(),
                            ),
                            Value::Array(
                                open.offered_capabilities
                                    .iter()
                                    .map(|c| Value::Symbol(c.clone().into_bytes()))
                                    .collect(),
                            ),
                            Value::Array(
                                open.desired_capabilities
                                    .iter()
                                    .map(|c| Value::Symbol(c.clone().into_bytes()))
                                    .collect(),
                            ),
                            Value::Map(BTreeMap::from_iter(
                                open.properties
                                    .iter()
                                    .map(|(k, v)| (Value::String(k.clone()), v.clone())),
                            )),
                        ];
                        encode_ref(&Value::List(args), &mut body)?;
                    }
                    Performative::Close(_close) => {
                        body.write_u8(0)?;
                        encode_ref(&Value::Ulong(0x18), &mut body)?;
                        // TODO
                        let args = vec![Value::List(Vec::new())];
                        encode_ref(&Value::List(args), &mut body)?;
                    }
                    Performative::Begin(begin) => {
                        body.write_u8(0)?;
                        encode_ref(&Value::Ulong(0x11), &mut body)?;
                        let remote_channel = begin
                            .remote_channel
                            .map_or_else(|| Value::Null, |c| Value::Ushort(c));
                        let args = vec![
                            remote_channel,
                            Value::Uint(begin.next_outgoing_id),
                            Value::Uint(begin.incoming_window),
                            Value::Uint(begin.outgoing_window),
                            Value::Uint(begin.handle_max),
                            Value::Array(
                                begin
                                    .offered_capabilities
                                    .iter()
                                    .map(|c| Value::Symbol(c.clone().into_bytes()))
                                    .collect(),
                            ),
                            Value::Array(
                                begin
                                    .desired_capabilities
                                    .iter()
                                    .map(|c| Value::Symbol(c.clone().into_bytes()))
                                    .collect(),
                            ),
                            Value::Map(BTreeMap::from_iter(
                                begin
                                    .properties
                                    .iter()
                                    .map(|(k, v)| (Value::String(k.clone()), v.clone())),
                            )),
                        ];
                        encode_ref(&Value::List(args), &mut body)?;
                    }
                    Performative::End(_end) => {
                        body.write_u8(0)?;
                        encode_ref(&Value::Ulong(0x18), &mut body)?;
                        // TODO
                        let args = vec![Value::List(Vec::new())];
                        encode_ref(&Value::List(args), &mut body)?;
                    }
                }
            }

            if let Some(payload) = payload {
                body.write(payload)?;
            }

            header.size += body.len() as u32;

            encode_header(&header, writer)?;
            writer.write_all(&body[..]);

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
        let mut doff = header.doff;
        while doff > 2 {
            stream.read_u32::<NetworkEndian>()?;
            doff -= 1;
        }
        let descriptor = decode(stream)?;
        let performative = match descriptor {
            Value::Ulong(0x10) => {
                let list = decode(stream)?;
                if let Value::List(args) = list {
                    let mut open = Open {
                        hostname: String::from("localhost"), // TODO: Set to my hostname if not found
                        ..Default::default()
                    };
                    let mut it = args.iter();
                    if let Some(container_id) = it.next() {
                        open.container_id = container_id.to_string();
                    }

                    if let Some(hostname) = it.next() {
                        open.hostname = hostname.try_to_string().unwrap_or_else(|| String::new());
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
                if let Value::List(args) = decode(stream)? {
                    if args.len() > 0 {
                        if let Value::Ulong(0x1D) = args[0] {
                            let list = decode(stream)?;
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
                let list = decode(stream)?;
                if let Value::List(args) = list {
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
                if let Value::List(args) = decode(stream)? {
                    if args.len() > 0 {
                        if let Value::Ulong(0x1D) = args[0] {
                            let list = decode(stream)?;
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
        }?;
        Ok(Frame::AMQP {
            channel: header.ext,
            performative: Some(performative),
            payload: None,
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
        let frm = Open {
            hostname: String::from("localhost"),
            ..Default::default()
        };

        assert_eq!(String::from("localhost"), frm.hostname);
        assert_eq!(36, frm.container_id.len());
        assert_eq!(4294967295, frm.max_frame_size);
        assert_eq!(65535, frm.channel_max);
    }
}
