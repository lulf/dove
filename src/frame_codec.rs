/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The frame_codec contains utility types for simplifying frame and composite type encoding and decoding.

use byteorder::NetworkEndian;
use byteorder::WriteBytesExt;
use std::io::Write;
use std::vec::Vec;

use crate::convert::*;
use crate::error::*;
use crate::types::*;

/**
 * An encoder helper type that provides convenient encoding of AMQP described list types,
 * such as frames.
 */
pub struct FrameEncoder {
    desc: Value,
    args: Vec<u8>,
    nelems: usize,
}

/**
 * A decoder helper type that provides convenient decoding of described list types,
 * such as frames and a few other AMQP types.
 */
#[allow(dead_code)]
pub struct FrameDecoder<'a> {
    desc: &'a Value,
    args: &'a mut Vec<Value>,
}

impl FrameEncoder {
    pub fn new(desc: Value) -> FrameEncoder {
        FrameEncoder {
            desc,
            args: Vec::new(),
            nelems: 0,
        }
    }

    pub fn encode_arg<T>(&mut self, arg: &T) -> Result<()>
    where
        T: Encoder,
    {
        arg.encode(&mut self.args)?;
        self.nelems += 1;
        Ok(())
    }
}

impl<'a> FrameDecoder<'a> {
    pub fn new(desc: &'a Value, input: &'a mut Value) -> Result<FrameDecoder<'a>> {
        if let Value::List(args) = input {
            Ok(FrameDecoder { desc, args })
        } else {
            Err(AmqpError::decode_error(Some(
                format!("Error decoding frame arguments: {:?}", input).as_str(),
            )))
        }
    }

    pub fn get_descriptor(&self) -> &'a Value {
        self.desc
    }

    pub fn decode_required<T: TryFromValue>(&mut self, value: &mut T) -> Result<()> {
        self.decode(value, true)
    }

    pub fn decode_optional<T: TryFromValue>(&mut self, value: &mut T) -> Result<()> {
        self.decode(value, false)
    }

    pub fn decode<T: TryFromValue>(&mut self, value: &mut T, required: bool) -> Result<()> {
        if self.args.is_empty() {
            if required {
                return Err(AmqpError::decode_error(Some("Unexpected end of list")));
            } else {
                return Ok(());
            }
        }
        let mut drained = self.args.drain(0..1);
        trace!("(Desc: {:?} Next arg to decode: {:?}", self.desc, drained);
        if let Some(arg) = drained.next() {
            let v = arg;
            *value = T::try_from(v)?;
        } else if required {
            return Err(AmqpError::decode_error(Some(
                "Decoded null value for required argument",
            )));
        }
        Ok(())
    }
}

impl Encoder for FrameEncoder {
    /**
     * Function duplicated from the list encoding to allow more efficient
     * encoding of frames.
     */
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        writer.write_u8(0)?;
        self.desc.encode(writer)?;
        if self.args.len() > LIST32_MAX {
            return Err(AmqpError::decode_error(Some(
                "Encoded list size cannot be longer than 4294967291 bytes",
            )));
        } else if self.args.len() > LIST8_MAX {
            writer.write_u8(TypeCode::List32 as u8)?;
            writer.write_u32::<NetworkEndian>((4 + self.args.len()) as u32)?;
            writer.write_u32::<NetworkEndian>(self.nelems as u32)?;
            writer.write_all(&self.args[..])?;
        } else if !self.args.is_empty() {
            writer.write_u8(TypeCode::List8 as u8)?;
            writer.write_u8((1 + self.args.len()) as u8)?;
            writer.write_u8(self.nelems as u8)?;
            writer.write_all(&self.args[..])?;
        } else {
            writer.write_u8(TypeCode::List0 as u8)?;
        }
        Ok(TypeCode::Described)
    }
}
