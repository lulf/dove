/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The decoding module contains AMQP 1.0 type decoders and rust native type decoders.

use byteorder::NetworkEndian;
use byteorder::ReadBytesExt;
use uuid::Uuid;
use std::io::Read;
use std::vec::Vec;

use crate::error::*;
use crate::frame_codec::*;
use crate::types::*;

/**
 * Decode an AMQP value from an byte reader. Reads the type constructor
 * first and passes this to the rest of the decoding function.
 */
pub fn decode_value(reader: &mut dyn Read) -> Result<Value> {
    let raw_code: u8 = reader.read_u8()?;
    decode_value_with_ctor(raw_code, reader)
}

/**
 * Decode an AMQP value from a byte reader based on the type constructor passed
 */
fn decode_value_with_ctor(raw_code: u8, reader: &mut dyn Read) -> Result<Value> {
    let code = decode_type(raw_code)?;
    match code {
        TypeCode::Described => {
            let descriptor = decode_value(reader)?;
            let value = decode_value(reader)?;
            Ok(Value::Described(Box::new(descriptor), Box::new(value)))
        }
        TypeCode::Null => Ok(Value::Null),
        TypeCode::Boolean => {
            let val = reader.read_u8()?;
            Ok(Value::Bool(val == 1))
        }
        TypeCode::BooleanTrue => Ok(Value::Bool(true)),
        TypeCode::BooleanFalse => Ok(Value::Bool(false)),
        TypeCode::Ubyte => {
            let val = reader.read_u8()?;
            Ok(Value::Ubyte(val))
        }
        TypeCode::Ushort => {
            let val = reader.read_u16::<NetworkEndian>()?;
            Ok(Value::Ushort(val))
        }
        TypeCode::Uint => {
            let val = reader.read_u32::<NetworkEndian>()?;
            Ok(Value::Uint(val))
        }
        TypeCode::Uintsmall => {
            let val = reader.read_u8()? as u32;
            Ok(Value::Uint(val))
        }
        TypeCode::Uint0 => Ok(Value::Uint(0)),
        TypeCode::Ulong => {
            let val = reader.read_u64::<NetworkEndian>()?;
            Ok(Value::Ulong(val))
        }
        TypeCode::Ulongsmall => {
            let val = reader.read_u8()? as u64;
            Ok(Value::Ulong(val))
        }
        TypeCode::Ulong0 => Ok(Value::Ulong(0)),
        TypeCode::Byte => {
            let val = reader.read_i8()?;
            Ok(Value::Byte(val))
        }
        TypeCode::Short => {
            let val = reader.read_i16::<NetworkEndian>()?;
            Ok(Value::Short(val))
        }
        TypeCode::Int => {
            let val = reader.read_i32::<NetworkEndian>()?;
            Ok(Value::Int(val))
        }
        TypeCode::Intsmall => {
            let val = reader.read_i8()? as i32;
            Ok(Value::Int(val))
        }
        TypeCode::Long => {
            let val = reader.read_i64::<NetworkEndian>()?;
            Ok(Value::Long(val))
        }
        TypeCode::Longsmall => {
            let val = reader.read_i8()? as i64;
            Ok(Value::Long(val))
        }
        TypeCode::Str8 => {
            let len = reader.read_u8()? as usize;
            let mut buffer = vec![0u8; len];
            reader.read_exact(&mut buffer)?;
            let s = String::from_utf8(buffer)?;
            Ok(Value::String(s))
        }
        TypeCode::Str32 => {
            let len = reader.read_u32::<NetworkEndian>()? as usize;
            let mut buffer = vec![0u8; len];
            reader.read_exact(&mut buffer)?;
            let s = String::from_utf8(buffer)?;
            Ok(Value::String(s))
        }
        TypeCode::Sym8 => {
            let len = reader.read_u8()? as usize;
            let mut buffer = vec![0u8; len];
            reader.read_exact(&mut buffer)?;
            Ok(Value::Symbol(buffer))
        }
        TypeCode::Sym32 => {
            let len = reader.read_u32::<NetworkEndian>()? as usize;
            let mut buffer = vec![0u8; len];
            reader.read_exact(&mut buffer)?;
            Ok(Value::Symbol(buffer))
        }
        TypeCode::Bin8 => {
            let len = reader.read_u8()? as usize;
            let mut buffer = vec![0u8; len];
            reader.read_exact(&mut buffer)?;
            Ok(Value::Binary(buffer))
        }
        TypeCode::Bin32 => {
            let len = reader.read_u32::<NetworkEndian>()? as usize;
            let mut buffer = vec![0u8; len];
            reader.read_exact(&mut buffer)?;
            Ok(Value::Binary(buffer))
        }
        TypeCode::List0 => Ok(Value::List(Vec::new())),
        TypeCode::List8 => {
            let _sz = reader.read_u8()? as usize;
            let count = reader.read_u8()? as usize;
            let mut data: Vec<Value> = Vec::new();
            for _num in 0..count {
                let result = decode_value(reader)?;
                data.push(result);
            }
            Ok(Value::List(data))
        }
        TypeCode::List32 => {
            let _sz = reader.read_u32::<NetworkEndian>()? as usize;
            let count = reader.read_u32::<NetworkEndian>()? as usize;
            let mut data: Vec<Value> = Vec::new();
            for _num in 0..count {
                let result = decode_value(reader)?;
                data.push(result);
            }
            Ok(Value::List(data))
        }
        TypeCode::Array8 => {
            let _sz = reader.read_u8()? as usize;
            let count = reader.read_u8()? as usize;
            let ctype = reader.read_u8()?;
            let mut data: Vec<Value> = Vec::new();
            for _num in 0..count {
                let result = decode_value_with_ctor(ctype, reader)?;
                data.push(result);
            }
            Ok(Value::Array(data))
        }
        TypeCode::Array32 => {
            let _sz = reader.read_u32::<NetworkEndian>()? as usize;
            let count = reader.read_u32::<NetworkEndian>()? as usize;
            let ctype = reader.read_u8()?;
            let mut data: Vec<Value> = Vec::new();
            for _num in 0..count {
                let result = decode_value_with_ctor(ctype, reader)?;
                data.push(result);
            }
            Ok(Value::Array(data))
        }
        TypeCode::Map8 => {
            let _sz = reader.read_u8()? as usize;
            let count = reader.read_u8()? as usize / 2;
            let mut data: Vec<(Value, Value)> = Vec::new();
            for _num in 0..count {
                let key = decode_value(reader)?;
                let value = decode_value(reader)?;
                data.push((key, value));
            }
            Ok(Value::Map(data))
        }
        TypeCode::Map32 => {
            let _sz = reader.read_u32::<NetworkEndian>()? as usize;
            let count = reader.read_u32::<NetworkEndian>()? as usize / 2;
            let mut data: Vec<(Value, Value)> = Vec::new();
            for _num in 0..count {
                let key = decode_value(reader)?;
                let value = decode_value(reader)?;
                data.push((key, value));
            }
            Ok(Value::Map(data))
        }
        TypeCode::Char => {
            let val = reader.read_u32::<NetworkEndian>()?;
            Ok(Value::Char(char::from_u32(val).ok_or_else(|| {
                AmqpError::decode_error(Some("Invalid unicode character received"))
            })?))
        }
        TypeCode::Timestamp => {
            let val = reader.read_u64::<NetworkEndian>()?;
            Ok(Value::Timestamp(val))
        }
        TypeCode::Uuid => {
            let mut buf = [0; 16];
            reader.read_exact(buf.as_mut())?;
            let uuid = Uuid::from_slice(&buf).unwrap();
            Ok(Value::Uuid(uuid))
        }
    }
}

/**
 * Converts a byte value to a type constructor.
 */
fn decode_type(code: u8) -> Result<TypeCode> {
    match code {
        0x00 => Ok(TypeCode::Described),
        0x40 => Ok(TypeCode::Null),
        0x56 => Ok(TypeCode::Boolean),
        0x41 => Ok(TypeCode::BooleanTrue),
        0x42 => Ok(TypeCode::BooleanFalse),
        0x50 => Ok(TypeCode::Ubyte),
        0x60 => Ok(TypeCode::Ushort),
        0x70 => Ok(TypeCode::Uint),
        0x52 => Ok(TypeCode::Uintsmall),
        0x43 => Ok(TypeCode::Uint0),
        0x80 => Ok(TypeCode::Ulong),
        0x53 => Ok(TypeCode::Ulongsmall),
        0x44 => Ok(TypeCode::Ulong0),
        0x51 => Ok(TypeCode::Byte),
        0x61 => Ok(TypeCode::Short),
        0x71 => Ok(TypeCode::Int),
        0x54 => Ok(TypeCode::Intsmall),
        0x81 => Ok(TypeCode::Long),
        0x55 => Ok(TypeCode::Longsmall),
        // float
        // double
        // decimal32
        // decimal64
        // decimal128
        0x73 => Ok(TypeCode::Char),
        0x83 => Ok(TypeCode::Timestamp),
        0x98 => Ok(TypeCode::Uuid),
        0xA0 => Ok(TypeCode::Bin8),
        0xA1 => Ok(TypeCode::Str8),
        0xA3 => Ok(TypeCode::Sym8),
        0xB0 => Ok(TypeCode::Bin32),
        0xB1 => Ok(TypeCode::Str32),
        0xB3 => Ok(TypeCode::Sym32),
        0x45 => Ok(TypeCode::List0),
        0xC0 => Ok(TypeCode::List8),
        0xD0 => Ok(TypeCode::List32),
        0xC1 => Ok(TypeCode::Map8),
        0xD1 => Ok(TypeCode::Map32),
        0xE0 => Ok(TypeCode::Array8),
        0xF0 => Ok(TypeCode::Array32),
        _ => Err(AmqpError::AmqpDecodeError(Some(format!(
            "Unknown type code: 0x{:X}",
            code
        )))),
    }
}

impl ErrorCondition {
    pub fn decode(mut decoder: FrameDecoder) -> Result<ErrorCondition> {
        let mut condition = ErrorCondition {
            condition: String::new(),
            description: String::new(),
        };
        decoder.decode_required(&mut condition.condition)?;
        decoder.decode_optional(&mut condition.description)?;
        Ok(condition)
    }
}
