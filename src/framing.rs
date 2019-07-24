/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use byteorder::NetworkEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use std::convert::From;
use std::io::Read;
use std::io::Write;
use std::vec::Vec;
use uuid::Uuid;

use crate::error::*;
use crate::types::*;

#[derive(Debug)]
pub struct Open {
    pub container_id: String,
    pub hostname: String,
    pub max_frame_size: u32,
    pub channel_max: u16,
    pub idle_timeout: u32,
}

impl Default for Open {
    fn default() -> Open {
        Open {
            container_id: Uuid::new_v4().to_string(),
            hostname: String::new(),
            max_frame_size: 4294967295,
            channel_max: 65535,
            idle_timeout: 0,
        }
    }
}

pub enum Performative {
    Open(Open),
}

#[derive(Debug)]
pub enum FrameType {
    AMQP,
    SASL,
}

pub struct Frame {
    pub frameType: FrameType,
    pub channel: u16,
    pub performative: Option<Performative>,
    pub payload: Option<Box<Vec<u8>>>,
}

pub fn encode_frame(frame: &Frame, stream: &mut Write) -> Result<usize> {
    let mut buf: Vec<u8> = Vec::new();
    let doff = 2;
    let mut sz = 8;

    buf.write_u8(doff)?;
    buf.write_u8(0)?;
    buf.write_u16::<NetworkEndian>(frame.channel)?;

    if let Some(performative) = &frame.performative {
        match performative {
            Performative::Open(open) => {
                buf.write_u8(0)?;
                sz += encode_ref(&Value::Ulong(0x10), &mut buf)? + 1;
                let args = vec![
                    Value::String(open.container_id.clone()),
                    Value::String(open.hostname.clone()),
                    Value::Uint(open.max_frame_size),
                    Value::Ushort(open.channel_max),
                ];
                sz += encode_ref(&Value::List(args), &mut buf)?;
            }
        }
    }

    if let Some(payload) = &frame.payload {
        sz += buf.write(payload)?;
    }

    stream.write_u32::<NetworkEndian>(sz as u32);
    stream.write(&buf[..]);

    Ok(sz)
}

pub fn decode_frame(stream: &mut Read) -> Result<Frame> {
    let _sz = stream.read_u32::<NetworkEndian>()? as usize;
    let mut doff = stream.read_u8()?;
    let frame_type = stream.read_u8()?;
    if frame_type == 0 {
        let channel = stream.read_u16::<NetworkEndian>()?;
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
                        open.hostname = hostname.to_string();
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
                        println!("{:?}", outgoing_locales);
                    }

                    if let Some(incoming_locales) = it.next() {
                        // TODO:
                    }

                    if let Some(offered_capabilities) = it.next() {
                        // TODO:
                    }

                    if let Some(desired_capabilities) = it.next() {
                        // TODO:
                    }

                    if let Some(properties) = it.next() {
                        // TODO:
                    }

                    Ok(Performative::Open(open))
                } else {
                    Err(AmqpError::DecodeError(String::from(
                        "Missing expected arguments for open performative",
                    )))
                }
            }
            v => Err(AmqpError::DecodeError(format!(
                "Unexpected descriptor value: {:?}",
                v
            ))),
        }?;
        Ok(Frame {
            channel: channel,
            frameType: FrameType::AMQP,
            performative: Some(performative),
            payload: None,
        })
    //} else if frame_type == 1 {
    // SASL
    } else {
        Err(AmqpError::DecodeError(format!(
            "Unknown frame type {}",
            frame_type
        )))
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
