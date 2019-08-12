/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use byteorder::NetworkEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use std::collections::BTreeMap;
use std::io::Read;
use std::io::Write;
use std::vec::Vec;

use crate::error::*;

#[derive(Clone, PartialEq, Debug, PartialOrd, Ord, Eq)]
pub enum Value {
    Null,
    Ubyte(u8),
    Ushort(u16),
    Uint(u32),
    Ulong(u64),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    String(String),
    Symbol(Vec<u8>),
    Array(Vec<Value>),
    List(Vec<Value>),
    Map(BTreeMap<Value, Value>),
}

impl Value {
    pub fn try_to_string(self: &Self) -> Option<String> {
        match self {
            Value::String(v) => Some(v.clone()),
            Value::Symbol(v) => Some(String::from_utf8(v.to_vec()).expect("Error decoding symbol")),
            _ => None,
        }
    }
    pub fn to_string(self: &Self) -> String {
        self.try_to_string().expect("Unexpected type!")
    }

    pub fn to_u32(self: &Self) -> u32 {
        match self {
            Value::Ushort(v) => (*v as u32),
            Value::Uint(v) => (*v as u32),
            _ => panic!("Unexpected type"),
        }
    }

    pub fn to_u16(self: &Self) -> u16 {
        match self {
            Value::Ushort(v) => (*v as u16),
            _ => panic!("Unexpected type"),
        }
    }

    pub fn try_to_u16(self: &Self) -> Option<u16> {
        match self {
            Value::Ushort(v) => Some(*v as u16),
            _ => None,
        }
    }
}

const U8_MAX: usize = std::u8::MAX as usize;
const I8_MAX: usize = std::i8::MAX as usize;
const LIST8_MAX: usize = (std::u8::MAX as usize) - 1;
const LIST32_MAX: usize = (std::u32::MAX as usize) - 4;

pub fn encode_ref(value: &Value, writer: &mut Write) -> Result<()> {
    encode_value(value, writer)?;
    Ok(())
}

fn encode_value(value: &Value, writer: &mut Write) -> Result<TypeCode> {
    match value {
        Value::Null => {
            writer.write_u8(TypeCode::Null as u8)?;
            Ok(TypeCode::Null)
        }
        Value::String(val) => {
            if val.len() > U8_MAX {
                writer.write_u8(TypeCode::Str32 as u8)?;
                writer.write_u32::<NetworkEndian>(val.len() as u32)?;
                writer.write(val.as_bytes())?;
                Ok(TypeCode::Str32)
            } else {
                writer.write_u8(TypeCode::Str8 as u8)?;
                writer.write_u8(val.len() as u8)?;
                writer.write(val.as_bytes())?;
                Ok(TypeCode::Str8)
            }
        }
        Value::Symbol(val) => {
            if val.len() > U8_MAX {
                writer.write_u8(TypeCode::Sym32 as u8)?;
                writer.write_u32::<NetworkEndian>(val.len() as u32)?;
                writer.write(&val[..])?;
                Ok(TypeCode::Sym32)
            } else {
                writer.write_u8(TypeCode::Sym8 as u8)?;
                writer.write_u8(val.len() as u8)?;
                writer.write(&val[..])?;
                Ok(TypeCode::Sym8)
            }
        }
        Value::Ubyte(val) => {
            writer.write_u8(TypeCode::Ubyte as u8)?;
            writer.write_u8(*val)?;
            Ok(TypeCode::Ubyte)
        }
        Value::Ushort(val) => {
            writer.write_u8(TypeCode::Ushort as u8)?;
            writer.write_u16::<NetworkEndian>(*val)?;
            Ok(TypeCode::Ushort)
        }
        Value::Uint(val) => {
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
        Value::Ulong(val) => {
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
        Value::Byte(val) => {
            writer.write_u8(TypeCode::Byte as u8)?;
            writer.write_i8(*val)?;
            Ok(TypeCode::Byte)
        }
        Value::Short(val) => {
            writer.write_u8(TypeCode::Short as u8)?;
            writer.write_i16::<NetworkEndian>(*val)?;
            Ok(TypeCode::Short)
        }
        Value::Int(val) => {
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
        Value::Long(val) => {
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
        Value::Array(vec) => {
            let mut arraybuf = Vec::new();
            let mut code = 0;
            for v in vec.iter() {
                let mut valuebuf = Vec::new();
                encode_ref(v, &mut valuebuf)?;
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
                writer.write(&arraybuf[..]);
                Ok(TypeCode::Array32)
            } else if arraybuf.len() > 0 {
                writer.write_u8(TypeCode::Array8 as u8)?;
                writer.write_u8((2 + arraybuf.len()) as u8)?;
                writer.write_u8(vec.len() as u8)?;
                writer.write_u8(code)?;
                writer.write(&arraybuf[..]);
                Ok(TypeCode::Array8)
            } else {
                writer.write_u8(TypeCode::Null as u8)?;
                Ok(TypeCode::Null)
            }
        }
        Value::List(vec) => {
            let mut listbuf = Vec::new();
            for v in vec.iter() {
                encode_ref(v, &mut listbuf)?;
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
                writer.write(&listbuf[..]);
                Ok(TypeCode::List32)
            } else if listbuf.len() > 0 {
                writer.write_u8(TypeCode::List8 as u8)?;
                writer.write_u8((1 + listbuf.len()) as u8)?;
                writer.write_u8(vec.len() as u8)?;
                writer.write(&listbuf[..]);
                Ok(TypeCode::List8)
            } else {
                writer.write_u8(TypeCode::List0 as u8)?;
                Ok(TypeCode::List0)
            }
        }
        Value::Map(m) => {
            let mut listbuf = Vec::new();
            for (key, value) in m {
                encode_ref(key, &mut listbuf)?;
                encode_ref(value, &mut listbuf)?;
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
                writer.write(&listbuf[..]);
                Ok(TypeCode::Map32)
            } else {
                writer.write_u8(TypeCode::Map8 as u8)?;
                writer.write_u8((1 + listbuf.len()) as u8)?;
                writer.write_u8(n_items as u8)?;
                writer.write(&listbuf[..]);
                Ok(TypeCode::Map8)
            }
        }
    }
}

pub fn decode(reader: &mut Read) -> Result<Value> {
    let raw_code: u8 = reader.read_u8()?;
    decode_with_ctor(raw_code, reader)
}
fn decode_with_ctor(raw_code: u8, reader: &mut Read) -> Result<Value> {
    if raw_code == 0 {
        let descriptor = decode(reader)?;
        Ok(descriptor)
    } else {
        let code = decode_type(raw_code)?;
        match code {
            TypeCode::Null => Ok(Value::Null),
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
            TypeCode::List0 => Ok(Value::List(Vec::new())),
            TypeCode::List8 => {
                let _sz = reader.read_u8()? as usize;
                let count = reader.read_u8()? as usize;
                let mut data: Vec<Value> = Vec::new();
                for _num in 0..count {
                    let result = decode(reader)?;
                    data.push(result);
                }
                Ok(Value::List(data))
            }
            TypeCode::List32 => {
                let _sz = reader.read_u32::<NetworkEndian>()? as usize;
                let count = reader.read_u32::<NetworkEndian>()? as usize;
                let mut data: Vec<Value> = Vec::new();
                for _num in 0..count {
                    let result = decode(reader)?;
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
                    let result = decode_with_ctor(ctype, reader)?;
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
                    let result = decode_with_ctor(ctype, reader)?;
                    data.push(result);
                }
                Ok(Value::Array(data))
            }
            TypeCode::Map8 => {
                let _sz = reader.read_u8()? as usize;
                let count = reader.read_u8()? as usize / 2;
                let mut data: BTreeMap<Value, Value> = BTreeMap::new();
                for _num in 0..count {
                    let key = decode(reader)?;
                    let value = decode(reader)?;
                    data.insert(key, value);
                }
                Ok(Value::Map(data))
            }
            TypeCode::Map32 => {
                let _sz = reader.read_u32::<NetworkEndian>()? as usize;
                let count = reader.read_u32::<NetworkEndian>()? as usize / 2;
                let mut data: BTreeMap<Value, Value> = BTreeMap::new();
                for _num in 0..count {
                    let key = decode(reader)?;
                    let value = decode(reader)?;
                    data.insert(key, value);
                }
                Ok(Value::Map(data))
            }
        }
    }
}

#[repr(u8)]
#[derive(Clone, PartialEq, Debug, PartialOrd)]
enum TypeCode {
    Null = 0x40,
    Ubyte = 0x50,
    Ushort = 0x60,
    Uint = 0x70,
    Uintsmall = 0x52,
    Uint0 = 0x43,
    Ulong = 0x80,
    Ulongsmall = 0x53,
    Ulong0 = 0x44,
    Byte = 0x51,
    Short = 0x61,
    Int = 0x71,
    Intsmall = 0x54,
    Long = 0x81,
    Longsmall = 0x55,
    Str8 = 0xA1,
    Str32 = 0xB1,
    Sym8 = 0xA3,
    Sym32 = 0xB3,
    List0 = 0x45,
    List8 = 0xC0,
    List32 = 0xD0,
    Map8 = 0xC1,
    Map32 = 0xD1,
    Array8 = 0xE0,
    Array32 = 0xF0,
}

fn decode_type(code: u8) -> Result<TypeCode> {
    match code {
        0x40 => Ok(TypeCode::Null),
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
        0xA1 => Ok(TypeCode::Str8),
        0xA3 => Ok(TypeCode::Sym8),
        0xB1 => Ok(TypeCode::Str32),
        0xB3 => Ok(TypeCode::Sym32),
        0x45 => Ok(TypeCode::List0),
        0xC0 => Ok(TypeCode::List8),
        0xD0 => Ok(TypeCode::List32),
        0xC1 => Ok(TypeCode::Map8),
        0xD1 => Ok(TypeCode::Map32),
        0xE0 => Ok(TypeCode::Array8),
        0xF0 => Ok(TypeCode::Array32),
        _ => Err(AmqpError::amqp_error(
            condition::DECODE_ERROR,
            Some(format!("Unknown type code: 0x{:X}", code).as_str()),
        )),
    }
}

// const TYPE_CODE_Bool = 0x56,
// const TYPE_CODE_Truebool = 0x41,
// const TYPE_CODE_Falsebool = 0x42,
// const TYPE_CODE_Ubyte = 0x50,
// const TYPE_CODE_Ushort = 0x60,
// const TYPE_CODE_Uint = 0x70,
// const TYPE_CODE_Smalluint = 0x52,
// const TYPE_CODE_Uint0 = 0x43,

// const TYPE_CODE_Byte = 0x51,
// const TYPE_CODE_Short = 0x61,
// const TYPE_CODE_Smallint = 0x54,
// const TYPE_CODE_Int = 0x71,
// const TYPE_CODE_Smalllong = 0x55,
// const TYPE_CODE_Long = 0x81,
// const TYPE_CODE_Float = 0x72,
// const TYPE_CODE_Double = 0x82,
// const TYPE_CODE_Decimal32 = 0x74,
// const TYPE_CODE_Decimal64 = 0x84,
// const TYPE_CODE_Decimal128 = 0x94,
// const TYPE_CODE_Char = 0x73,
// const TYPE_CODE_Timestamp = 0x83,
// const TYPE_CODE_Uuid = 0x98,
// const TYPE_CODE_Vbin8 = 0xA0,
// const TYPE_CODE_Vbin32 = 0xB0,
// const TYPE_CODE_Str8 = 0xA1,
// const TYPE_CODE_Str32 = 0xB1,
// const TYPE_CODE_Sym8 = 0xA3,
// const TYPE_CODE_Sym32 = 0xB3,
// const TYPE_CODE_List0 = 0x45,
// const TYPE_CODE_List8 = 0xC0,
// const TYPE_CODE_List32 = 0xD0,
// const TYPE_CODE_Map8 = 0xC1,
// const TYPE_CODE_Map32 = 0xD1,
//const TYPE_CODE_Array8 = 0xE0,
//const TYPE_CODE_Array32 = 0xF0,

/*
fn decode_value<T: AmqpCodec<T>>(stream: &mut Read) -> Result<T> {
    let code = stream.read_u8()?;
    return T::decode(code, stream);
}
*/

/*
fn encode_value(amqp_value: &AmqpValue, stream: &mut Write) -> Result<()> {
    match amqp_value {
        AmqpValue::Null => encode_type(TypeCode::Null, stream)?,
        AmqpValue::Boolean(value) => {
            encode_type(
                if *value {
                    TypeCode::Truebool
                } else {
                    TypeCode::Falsebool
                },
                stream,
            )?;
        }
        AmqpValue::Ubyte(value) => {
            encode_type(TypeCode::Ubyte, stream)?;
            stream.write_u8(*value)?;
        }
        AmqpValue::Ushort(value) => {
            encode_type(TypeCode::Ushort, stream)?;
            stream.write_u16::<NetworkEndian>(*value)?;
        }
        AmqpValue::Uint(value) => {
            encode_type(TypeCode::Uint, stream)?;
            stream.write_u32::<NetworkEndian>(*value)?;
        }
        AmqpValue::Ulong(value) => {
            if *value > 0xFF {
                encode_type(TypeCode::Ulong, stream)?;
                stream.write_u64::<NetworkEndian>(*value)?;
            } else if *value > 0 {
                encode_type(TypeCode::Smallulong, stream)?;
                stream.write_u8(*value as u8);
            } else {
                encode_type(TypeCode::Ulong0, stream)?;
            }
        }
        AmqpValue::Byte(value) => {
            encode_type(TypeCode::Byte, stream)?;
            stream.write_i8(*value)?;
        }
        AmqpValue::Short(value) => {
            encode_type(TypeCode::Short, stream)?;
            stream.write_i16::<NetworkEndian>(*value)?;
        }
        AmqpValue::Int(value) => {
            encode_type(TypeCode::Int, stream)?;
            stream.write_i32::<NetworkEndian>(*value)?;
        }
        AmqpValue::Long(value) => {
            encode_type(TypeCode::Long, stream)?;
            stream.write_i64::<NetworkEndian>(*value)?;
        }
        AmqpValue::Float(value) => {
            encode_type(TypeCode::Float, stream)?;
            stream.write_f32::<NetworkEndian>(*value)?;
        }
        AmqpValue::Double(value) => {
            encode_type(TypeCode::Double, stream)?;
            stream.write_f64::<NetworkEndian>(*value)?;
        }
        AmqpValue::Decimal32(value) => {
            encode_type(TypeCode::Decimal32, stream)?;
            stream.write_u32::<NetworkEndian>(*value)?;
        }
        AmqpValue::Decimal64(value) => {
            encode_type(TypeCode::Decimal64, stream)?;
            stream.write_u64::<NetworkEndian>(*value)?;
        }
        AmqpValue::Decimal128(value) => {
            encode_type(TypeCode::Decimal128, stream)?;
            stream.write_u64::<NetworkEndian>(value[0])?;
            stream.write_u64::<NetworkEndian>(value[1])?;
        }
        AmqpValue::Char(value) => {
            encode_type(TypeCode::Char, stream)?;
            // stream.write_u32::<NetworkEndian>(value)?;
        }
        AmqpValue::Timestamp(value) => {
            encode_type(TypeCode::Timestamp, stream)?;
            stream.write_i64::<NetworkEndian>(*value)?;
        }
        AmqpValue::Uuid(value) => {
            encode_type(TypeCode::Uuid, stream)?;
            stream.write(value)?;
        }
        AmqpValue::Binary(value) => {
            if (*value).len() > 0xFFFFFFFF {
                return Err(AmqpError::new(
                    "Binary values cannot be longer than 4294967295 octets",
                ));
            } else if (*value).len() > 0xFF {
                encode_type(TypeCode::Vbin32, stream)?;
                stream.write_u32::<NetworkEndian>((*value).len() as u32)?;
                stream.write(&*value)?;
            } else {
                encode_type(TypeCode::Vbin8, stream)?;
                stream.write_u8((*value).len() as u8)?;
                stream.write(&*value)?;
            }
        }
        AmqpValue::String(value) => {
            if value.len() > 0xFFFFFFFF {
                return Err(AmqpError::new(
                    "String values cannot be longer than 4294967295 octets",
                ));
            } else if value.len() > 0xFF {
                encode_type(TypeCode::Str32, stream)?;
                stream.write_u32::<NetworkEndian>(value.len() as u32);
                stream.write(value.as_bytes())?;
            } else {
                encode_type(TypeCode::Str8, stream)?;
                stream.write_u8(value.len() as u8);
                stream.write(value.as_bytes())?;
            }
        }
        AmqpValue::Symbol(value) => {
            if value.len() > 0xFFFFFFFF {
                return Err(AmqpError::new(
                    "Symbol values cannot be longer than 4294967295 octets",
                ));
            } else if value.len() > 0xFF {
                encode_type(TypeCode::Sym32, stream)?;
                stream.write_u32::<NetworkEndian>(value.len() as u32);
                stream.write(value.as_bytes())?;
            } else {
                encode_type(TypeCode::Sym8, stream)?;
                stream.write_u8(value.len() as u8);
                stream.write(value.as_bytes())?;
            }
        }
        AmqpValue::List(value) => {
            if value.len() > 0xFFFFFFFF {
                return Err(AmqpError::new(
                    "List length cannot be longer than 4294967295 items",
                ));
            } else if value.len() > 0 {
                encode_type(TypeCode::List32, stream)?;
                stream.write_u32::<NetworkEndian>(encoded_size(amqp_value) - 5)?;
                stream.write_u32::<NetworkEndian>(value.len() as u32)?;
                for v in value.iter() {
                    encode_value(&v, stream)?;
                }
            /*
            } else if value.len() > 0 {
                encode_type(TypeCode::List8, stream)?;
                stream.write_u8((encoded_size(amqp_value) - 2) as u8)?;
                stream.write_u8(value.len() as u8)?;
                for v in value.iter() {
                    encode_value(&v, stream)?;
                }
                */
            } else {
                encode_type(TypeCode::List0, stream)?;
            }
        }
        AmqpValue::Map(value) => {
            encode_type(TypeCode::Null, stream)?;
        }
        AmqpValue::Array(value) => {
            if value.len() > 0xFFFFFFFF {
                return Err(AmqpError::new(
                    "Array length cannot be longer than 4294967295 bytes",
                ));
            } else if value.len() > 0xFF {
                encode_type(TypeCode::Array32, stream)?;
            } else if value.len() > 0 {
                encode_type(TypeCode::Array8, stream)?;
            } else {
                encode_type(TypeCode::Null, stream)?;
            }
        }
        AmqpValue::Milliseconds(value) => {
            encode_type(TypeCode::Uint, stream)?;
            stream.write_u32::<NetworkEndian>(*value)?;
        }
        AmqpValue::IetfLanguageTag(value) => {}
        AmqpValue::Fields(value) => {}
    }
    return Ok(());
}
*/

#[cfg(test)]
mod tests {

    use super::*;
    use crate::error::*;

    fn assert_type(value: &Value, expected_len: usize, expected_type: TypeCode) {
        let mut output: Vec<u8> = Vec::new();
        let ctype = encode_value(value, &mut output).unwrap();
        assert_eq!(expected_type, ctype);
        assert_eq!(expected_len, output.len());

        let decoded = decode(&mut &output[..]).unwrap();
        assert_eq!(&decoded, value);
    }

    #[test]
    fn check_types() {
        assert_type(&Value::Ulong(123), 2, TypeCode::Ulongsmall);
        assert_type(&Value::Ulong(1234), 9, TypeCode::Ulong);
        assert_type(
            &Value::String(String::from("Hello, world")),
            14,
            TypeCode::Str8,
        );
        assert_type(&Value::String(String::from("aaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccdddddddddddddddddddddddddeeeeeeeeeeeeeeeeeeeeeeeeeffffffffffffffffffffgggggggggggggggggggggggghhhhhhhhhhhhhhhhhhhhhhhiiiiiiiiiiiiiiiiiiiiiiiijjjjjjjjjjjjjjjjjjjkkkkkkkkkkkkkkkkkkkkkkllllllllllllllllllllmmmmmmmmmmmmmmmmmmmmnnnnnnnnnnnnnnnnnnnnooooooooooooooooooooppppppppppppppppppqqqqqqqqqqqqqqqq")), 370, TypeCode::Str32);
        assert_type(
            &Value::List(vec![
                Value::Ulong(1),
                Value::Ulong(42),
                Value::String(String::from("Hello, world")),
            ]),
            21,
            TypeCode::List8,
        );
    }
}
