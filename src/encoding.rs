/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The encoding module contains AMQP 1.0 type encoders and rust native types encoders.

use byteorder::NetworkEndian;
use byteorder::WriteBytesExt;
use std::collections::BTreeMap;
use std::io::Write;
use std::iter::FromIterator;
use std::vec::Vec;

use crate::error::*;
use crate::frame_codec::*;
use crate::symbol::*;
use crate::types::*;

/**
 * This is the main encoder implementation for AMQP types.
 */
impl Encoder for ValueRef<'_> {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let value = self;
        match *value {
            ValueRef::Described(ref descriptor, ref value) => {
                writer.write_u8(0)?;
                descriptor.encode(writer)?;
                value.encode(writer)?;
                Ok(TypeCode::Described)
            }
            ValueRef::Null => {
                writer.write_u8(TypeCode::Null as u8)?;
                Ok(TypeCode::Null)
            }
            ValueRef::Bool(value) => {
                let code = if *value {
                    TypeCode::Booleantrue
                } else {
                    TypeCode::Booleanfalse
                };
                writer.write_u8(code as u8)?;

                Ok(code)
            }
            ValueRef::String(val) => {
                if val.len() > U8_MAX {
                    writer.write_u8(TypeCode::Str32 as u8)?;
                    writer.write_u32::<NetworkEndian>(val.len() as u32)?;
                    writer.write_all(val.as_bytes())?;
                    Ok(TypeCode::Str32)
                } else {
                    writer.write_u8(TypeCode::Str8 as u8)?;
                    writer.write_u8(val.len() as u8)?;
                    writer.write_all(val.as_bytes())?;
                    Ok(TypeCode::Str8)
                }
            }
            ValueRef::SymbolRef(val) => {
                if val.len() > U8_MAX {
                    writer.write_u8(TypeCode::Sym32 as u8)?;
                    writer.write_u32::<NetworkEndian>(val.len() as u32)?;
                    writer.write_all(&val.as_bytes()[..])?;
                    Ok(TypeCode::Sym32)
                } else {
                    writer.write_u8(TypeCode::Sym8 as u8)?;
                    writer.write_u8(val.len() as u8)?;
                    writer.write_all(&val.as_bytes()[..])?;
                    Ok(TypeCode::Sym8)
                }
            }
            ValueRef::Symbol(val) => {
                if val.len() > U8_MAX {
                    writer.write_u8(TypeCode::Sym32 as u8)?;
                    writer.write_u32::<NetworkEndian>(val.len() as u32)?;
                    writer.write_all(val)?;
                    Ok(TypeCode::Sym32)
                } else {
                    writer.write_u8(TypeCode::Sym8 as u8)?;
                    writer.write_u8(val.len() as u8)?;
                    writer.write_all(val)?;
                    Ok(TypeCode::Sym8)
                }
            }
            ValueRef::Binary(val) => {
                if val.len() > U8_MAX {
                    writer.write_u8(TypeCode::Bin32 as u8)?;
                    writer.write_u32::<NetworkEndian>(val.len() as u32)?;
                    writer.write_all(&val[..])?;
                    Ok(TypeCode::Bin32)
                } else {
                    writer.write_u8(TypeCode::Bin8 as u8)?;
                    writer.write_u8(val.len() as u8)?;
                    writer.write_all(&val[..])?;
                    Ok(TypeCode::Bin8)
                }
            }
            ValueRef::Ubyte(val) => {
                writer.write_u8(TypeCode::Ubyte as u8)?;
                writer.write_u8(*val)?;
                Ok(TypeCode::Ubyte)
            }
            ValueRef::Ushort(val) => {
                writer.write_u8(TypeCode::Ushort as u8)?;
                writer.write_u16::<NetworkEndian>(*val)?;
                Ok(TypeCode::Ushort)
            }
            ValueRef::Uint(val) => {
                if *val > U8_MAX as u32 {
                    writer.write_u8(TypeCode::Uint as u8)?;
                    writer.write_u32::<NetworkEndian>(*val)?;
                    Ok(TypeCode::Uint)
                } else if *val > 0 {
                    writer.write_u8(TypeCode::Uintsmall as u8)?;
                    writer.write_u8(*val as u8)?;
                    Ok(TypeCode::Uintsmall)
                } else {
                    writer.write_u8(TypeCode::Uint0 as u8)?;
                    Ok(TypeCode::Uint0)
                }
            }
            ValueRef::Ulong(val) => {
                if *val > U8_MAX as u64 {
                    writer.write_u8(TypeCode::Ulong as u8)?;
                    writer.write_u64::<NetworkEndian>(*val)?;
                    Ok(TypeCode::Ulong)
                } else if *val > 0 {
                    writer.write_u8(TypeCode::Ulongsmall as u8)?;
                    writer.write_u8(*val as u8)?;
                    Ok(TypeCode::Ulongsmall)
                } else {
                    writer.write_u8(TypeCode::Ulong0 as u8)?;
                    Ok(TypeCode::Ulong0)
                }
            }
            ValueRef::Byte(val) => {
                writer.write_u8(TypeCode::Byte as u8)?;
                writer.write_i8(*val)?;
                Ok(TypeCode::Byte)
            }
            ValueRef::Short(val) => {
                writer.write_u8(TypeCode::Short as u8)?;
                writer.write_i16::<NetworkEndian>(*val)?;
                Ok(TypeCode::Short)
            }
            ValueRef::Int(val) => {
                if *val > I8_MAX as i32 {
                    writer.write_u8(TypeCode::Int as u8)?;
                    writer.write_i32::<NetworkEndian>(*val)?;
                    Ok(TypeCode::Int)
                } else {
                    writer.write_u8(TypeCode::Intsmall as u8)?;
                    writer.write_i8(*val as i8)?;
                    Ok(TypeCode::Intsmall)
                }
            }
            ValueRef::Long(val) => {
                if *val > I8_MAX as i64 {
                    writer.write_u8(TypeCode::Long as u8)?;
                    writer.write_i64::<NetworkEndian>(*val)?;
                    Ok(TypeCode::Long)
                } else {
                    writer.write_u8(TypeCode::Longsmall as u8)?;
                    writer.write_i8(*val as i8)?;
                    Ok(TypeCode::Longsmall)
                }
            }
            ValueRef::Array(vec) => {
                let mut arraybuf = Vec::new();
                let mut code = 0;
                for v in vec.iter() {
                    let mut valuebuf = Vec::new();
                    v.encode(&mut valuebuf)?;
                    if code == 0 {
                        code = valuebuf[0];
                    }
                    arraybuf.extend_from_slice(&valuebuf[1..]);
                }

                if arraybuf.len() > LIST32_MAX {
                    Err(AmqpError::amqp_error(
                        condition::DECODE_ERROR,
                        Some("Encoded array size cannot be longer than 4294967291 bytes"),
                    ))
                } else if arraybuf.len() > LIST8_MAX {
                    writer.write_u8(TypeCode::Array32 as u8)?;
                    writer.write_u32::<NetworkEndian>((5 + arraybuf.len()) as u32)?;
                    writer.write_u32::<NetworkEndian>(vec.len() as u32)?;
                    writer.write_u8(code)?;
                    writer.write_all(&arraybuf[..])?;
                    Ok(TypeCode::Array32)
                } else if !arraybuf.is_empty() {
                    writer.write_u8(TypeCode::Array8 as u8)?;
                    writer.write_u8((2 + arraybuf.len()) as u8)?;
                    writer.write_u8(vec.len() as u8)?;
                    writer.write_u8(code)?;
                    writer.write_all(&arraybuf[..])?;
                    Ok(TypeCode::Array8)
                } else {
                    writer.write_u8(TypeCode::Null as u8)?;
                    Ok(TypeCode::Null)
                }
            }
            ValueRef::List(vec) => {
                let mut listbuf = Vec::new();
                for v in vec.iter() {
                    v.encode(&mut listbuf)?;
                }

                if listbuf.len() > LIST32_MAX {
                    Err(AmqpError::amqp_error(
                        condition::DECODE_ERROR,
                        Some("Encoded list size cannot be longer than 4294967291 bytes"),
                    ))
                } else if listbuf.len() > LIST8_MAX {
                    writer.write_u8(TypeCode::List32 as u8)?;
                    writer.write_u32::<NetworkEndian>((4 + listbuf.len()) as u32)?;
                    writer.write_u32::<NetworkEndian>(vec.len() as u32)?;
                    writer.write_all(&listbuf[..])?;
                    Ok(TypeCode::List32)
                } else if !listbuf.is_empty() {
                    writer.write_u8(TypeCode::List8 as u8)?;
                    writer.write_u8((1 + listbuf.len()) as u8)?;
                    writer.write_u8(vec.len() as u8)?;
                    writer.write_all(&listbuf[..])?;
                    Ok(TypeCode::List8)
                } else {
                    writer.write_u8(TypeCode::List0 as u8)?;
                    Ok(TypeCode::List0)
                }
            }
            ValueRef::Map(m) => {
                let mut listbuf = Vec::new();
                for (key, value) in m {
                    key.encode(&mut listbuf)?;
                    value.encode(&mut listbuf)?;
                }

                let n_items = m.len() * 2;

                if listbuf.len() > LIST32_MAX {
                    Err(AmqpError::amqp_error(
                        condition::DECODE_ERROR,
                        Some("Encoded map size cannot be longer than 4294967291 bytes"),
                    ))
                } else if listbuf.len() > LIST8_MAX || n_items > U8_MAX {
                    writer.write_u8(TypeCode::Map32 as u8)?;
                    writer.write_u32::<NetworkEndian>((4 + listbuf.len()) as u32)?;
                    writer.write_u32::<NetworkEndian>(n_items as u32)?;
                    writer.write_all(&listbuf[..])?;
                    Ok(TypeCode::Map32)
                } else {
                    writer.write_u8(TypeCode::Map8 as u8)?;
                    writer.write_u8((1 + listbuf.len()) as u8)?;
                    writer.write_u8(n_items as u8)?;
                    writer.write_all(&listbuf[..])?;
                    Ok(TypeCode::Map8)
                }
            }
            ValueRef::ArrayRef(vec) => {
                let mut arraybuf = Vec::new();
                let mut code = 0;
                for v in vec.iter() {
                    let mut valuebuf = Vec::new();
                    v.encode(&mut valuebuf)?;
                    if code == 0 {
                        code = valuebuf[0];
                    }
                    arraybuf.extend_from_slice(&valuebuf[1..]);
                }

                if arraybuf.len() > LIST32_MAX {
                    Err(AmqpError::amqp_error(
                        condition::DECODE_ERROR,
                        Some("Encoded array size cannot be longer than 4294967291 bytes"),
                    ))
                } else if arraybuf.len() > LIST8_MAX {
                    writer.write_u8(TypeCode::Array32 as u8)?;
                    writer.write_u32::<NetworkEndian>((5 + arraybuf.len()) as u32)?;
                    writer.write_u32::<NetworkEndian>(vec.len() as u32)?;
                    writer.write_u8(code)?;
                    writer.write_all(&arraybuf[..])?;
                    Ok(TypeCode::Array32)
                } else if !arraybuf.is_empty() {
                    writer.write_u8(TypeCode::Array8 as u8)?;
                    writer.write_u8((2 + arraybuf.len()) as u8)?;
                    writer.write_u8(vec.len() as u8)?;
                    writer.write_u8(code)?;
                    writer.write_all(&arraybuf[..])?;
                    Ok(TypeCode::Array8)
                } else {
                    writer.write_u8(TypeCode::Null as u8)?;
                    Ok(TypeCode::Null)
                }
            }
            ValueRef::ListRef(vec) => {
                let mut listbuf = Vec::new();
                for v in vec.iter() {
                    v.encode(&mut listbuf)?;
                }

                if listbuf.len() > LIST32_MAX {
                    Err(AmqpError::amqp_error(
                        condition::DECODE_ERROR,
                        Some("Encoded list size cannot be longer than 4294967291 bytes"),
                    ))
                } else if listbuf.len() > LIST8_MAX {
                    writer.write_u8(TypeCode::List32 as u8)?;
                    writer.write_u32::<NetworkEndian>((4 + listbuf.len()) as u32)?;
                    writer.write_u32::<NetworkEndian>(vec.len() as u32)?;
                    writer.write_all(&listbuf[..])?;
                    Ok(TypeCode::List32)
                } else if !listbuf.is_empty() {
                    writer.write_u8(TypeCode::List8 as u8)?;
                    writer.write_u8((1 + listbuf.len()) as u8)?;
                    writer.write_u8(vec.len() as u8)?;
                    writer.write_all(&listbuf[..])?;
                    Ok(TypeCode::List8)
                } else {
                    writer.write_u8(TypeCode::List0 as u8)?;
                    Ok(TypeCode::List0)
                }
            }
            ValueRef::MapRef(m) => {
                let mut listbuf = Vec::new();
                for (key, value) in m {
                    key.encode(&mut listbuf)?;
                    value.encode(&mut listbuf)?;
                }

                let n_items = m.len() * 2;

                if listbuf.len() > LIST32_MAX {
                    Err(AmqpError::amqp_error(
                        condition::DECODE_ERROR,
                        Some("Encoded map size cannot be longer than 4294967291 bytes"),
                    ))
                } else if listbuf.len() > LIST8_MAX || n_items > U8_MAX {
                    writer.write_u8(TypeCode::Map32 as u8)?;
                    writer.write_u32::<NetworkEndian>((4 + listbuf.len()) as u32)?;
                    writer.write_u32::<NetworkEndian>(n_items as u32)?;
                    writer.write_all(&listbuf[..])?;
                    Ok(TypeCode::Map32)
                } else {
                    writer.write_u8(TypeCode::Map8 as u8)?;
                    writer.write_u8((1 + listbuf.len()) as u8)?;
                    writer.write_u8(n_items as u8)?;
                    writer.write_all(&listbuf[..])?;
                    Ok(TypeCode::Map8)
                }
            }
        }
    }
}

impl Encoder for Value {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let value = self;
        value.value_ref().encode(writer)
    }
}

impl Encoder for ErrorCondition {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_ERROR);
        encoder.encode_arg(&self.condition)?;
        encoder.encode_arg(&self.description)?;
        encoder.encode(writer)
    }
}

/**
 * Encoders for native rust types.
 */
impl Encoder for Vec<String> {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut values = Vec::new();
        for s in self.iter() {
            values.push(ValueRef::String(s));
        }
        ValueRef::ArrayRef(&values).encode(writer)
    }
}

impl Encoder for Vec<u8> {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::Binary(self).encode(writer)
    }
}

impl Encoder for &[u8] {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::Binary(self).encode(writer)
    }
}

impl Encoder for String {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::String(self).encode(writer)
    }
}

impl Encoder for bool {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::Bool(self).encode(writer)
    }
}

impl Encoder for u64 {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::Ulong(self).encode(writer)
    }
}

impl Encoder for u32 {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::Uint(self).encode(writer)
    }
}

impl Encoder for u16 {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::Ushort(self).encode(writer)
    }
}

impl Encoder for u8 {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::Ubyte(self).encode(writer)
    }
}

impl<T: Encoder> Encoder for Option<T> {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        match self {
            Some(value) => value.encode(writer),
            _ => Value::Null.encode(writer),
        }
    }
}

impl Encoder for BTreeMap<String, Value> {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let m = BTreeMap::from_iter(
            self.iter()
                .map(|(k, v)| (ValueRef::String(k), v.value_ref())),
        );
        ValueRef::MapRef(&m).encode(writer)
    }
}

impl Encoder for BTreeMap<Value, Value> {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let m = BTreeMap::from_iter(self.iter().map(|(k, v)| (k.value_ref(), v.value_ref())));
        ValueRef::MapRef(&m).encode(writer)
    }
}

impl Encoder for BTreeMap<Symbol, Value> {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let m = BTreeMap::from_iter(
            self.iter()
                .map(|(k, v)| (ValueRef::Symbol(k.to_slice()), v.value_ref())),
        );
        ValueRef::MapRef(&m).encode(writer)
    }
}
