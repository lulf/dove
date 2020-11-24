/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The types module contains the AMQP 1.0 types system encoders and decoders. By using these types you can enforce a certain encoding for your data.

use log::trace;
use std::collections::BTreeMap;
use std::io::Write;
use std::vec::Vec;

use crate::error::*;

/**
 * Encoder trait that all types that can be serialized to an AMQP type must implement.
 */
pub trait Encoder {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode>;
}

// Various constants used in encoding and decoding.
pub const U8_MAX: usize = std::u8::MAX as usize;
pub const I8_MAX: usize = std::i8::MAX as usize;
pub const LIST8_MAX: usize = (std::u8::MAX as usize) - 1;
pub const LIST32_MAX: usize = (std::u32::MAX as usize) - 4;

pub const DESC_OPEN: Value = Value::Ulong(0x10);
pub const DESC_BEGIN: Value = Value::Ulong(0x11);
pub const DESC_ATTACH: Value = Value::Ulong(0x12);
pub const DESC_FLOW: Value = Value::Ulong(0x13);
pub const DESC_TRANSFER: Value = Value::Ulong(0x14);
pub const DESC_DISPOSITION: Value = Value::Ulong(0x15);
pub const DESC_SOURCE: Value = Value::Ulong(0x28);
pub const DESC_TARGET: Value = Value::Ulong(0x29);

pub const DESC_MESSAGE_HEADER: Value = Value::Ulong(0x70);
pub const DESC_MESSAGE_DELIVERY_ANNOTATIONS: Value = Value::Ulong(0x71);
pub const DESC_MESSAGE_ANNOTATIONS: Value = Value::Ulong(0x72);
pub const DESC_MESSAGE_PROPERTIES: Value = Value::Ulong(0x73);
pub const DESC_MESSAGE_APPLICATION_PROPERTIES: Value = Value::Ulong(0x74);
pub const DESC_MESSAGE_AMQP_DATA: Value = Value::Ulong(0x75);
pub const DESC_MESSAGE_AMQP_SEQUENCE: Value = Value::Ulong(0x76);
pub const DESC_MESSAGE_AMQP_VALUE: Value = Value::Ulong(0x77);
pub const DESC_MESSAGE_FOOTER: Value = Value::Ulong(0x78);

pub const DESC_DELIVERY_STATE_RECEIVED: Value = Value::Ulong(0x23);
pub const DESC_DELIVERY_STATE_ACCEPTED: Value = Value::Ulong(0x24);
pub const DESC_DELIVERY_STATE_REJECTED: Value = Value::Ulong(0x25);
pub const DESC_DELIVERY_STATE_RELEASED: Value = Value::Ulong(0x26);
pub const DESC_DELIVERY_STATE_MODIFIED: Value = Value::Ulong(0x27);

pub const DESC_DETACH: Value = Value::Ulong(0x16);
pub const DESC_END: Value = Value::Ulong(0x17);
pub const DESC_CLOSE: Value = Value::Ulong(0x18);

pub const DESC_SASL_MECHANISMS: Value = Value::Ulong(0x40);
pub const DESC_SASL_INIT: Value = Value::Ulong(0x41);
pub const DESC_SASL_OUTCOME: Value = Value::Ulong(0x44);
pub const DESC_ERROR: Value = Value::Ulong(0x1D);

/**
 * A reference to a type with a given value. This allows efficient zero copy of the provided values and should
 * be used when possible (depends on lifetime constraints where its used).
 */
#[derive(Clone, PartialEq, Debug, PartialOrd, Ord, Eq)]
pub enum ValueRef<'a> {
    Described(Box<ValueRef<'a>>, Box<ValueRef<'a>>),
    Null,
    Bool(&'a bool),
    Ubyte(&'a u8),
    Ushort(&'a u16),
    Uint(&'a u32),
    Ulong(&'a u64),
    Byte(&'a i8),
    Short(&'a i16),
    Int(&'a i32),
    Long(&'a i64),
    String(&'a str),
    Binary(&'a [u8]),
    Symbol(&'a [u8]),
    SymbolRef(&'a str),
    Array(&'a Vec<Value>),
    List(&'a Vec<Value>),
    Map(&'a BTreeMap<Value, Value>),
    ArrayRef(&'a Vec<ValueRef<'a>>),
    ListRef(&'a Vec<ValueRef<'a>>),
    MapRef(&'a BTreeMap<ValueRef<'a>, ValueRef<'a>>),
}

/**
 * An owned value type, mostly used during decoding.
 */
#[derive(Clone, PartialEq, Debug, PartialOrd, Ord, Eq)]
pub enum Value {
    Described(Box<Value>, Box<Value>),
    Null,
    Bool(bool),
    Ubyte(u8),
    Ushort(u16),
    Uint(u32),
    Ulong(u64),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    String(String),
    Binary(Vec<u8>),
    Symbol(Vec<u8>),
    Array(Vec<Value>),
    List(Vec<Value>),
    Map(BTreeMap<Value, Value>),
}

impl Value {
    /**
     * Convert to a reference type. Not currently implemented for all types.
     */
    pub fn value_ref(&self) -> ValueRef {
        match self {
            Value::Described(ref descriptor, ref value) => ValueRef::Described(
                Box::new(descriptor.value_ref()),
                Box::new(value.value_ref()),
            ),
            Value::Null => ValueRef::Null,
            Value::Bool(ref value) => ValueRef::Bool(value),
            Value::String(ref value) => ValueRef::String(value),
            Value::Symbol(ref value) => ValueRef::Symbol(&value[..]),
            Value::List(ref value) => ValueRef::List(value),
            Value::Ubyte(ref value) => ValueRef::Ubyte(value),
            Value::Ushort(ref value) => ValueRef::Ushort(value),
            Value::Uint(ref value) => ValueRef::Uint(value),
            Value::Ulong(ref value) => ValueRef::Ulong(value),
            Value::Byte(ref value) => ValueRef::Byte(value),
            Value::Short(ref value) => ValueRef::Short(value),
            Value::Int(ref value) => ValueRef::Int(value),
            Value::Long(ref value) => ValueRef::Long(value),
            v => {
                trace!("Cannot convert value ref {:?}", v);
                panic!(format!("Cannot convert value ref {:?}", v));
            }
        }
    }
}

/**
 * All basic type codes in AMQP.
 */
#[repr(u8)]
#[derive(Clone, PartialEq, Debug, PartialOrd, Copy)]
pub enum TypeCode {
    Described = 0x00,
    Null = 0x40,
    Boolean = 0x56,
    Booleantrue = 0x41,
    Booleanfalse = 0x42,
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
    Bin8 = 0xA0,
    Bin32 = 0xB0,
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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decoding::decode_value;

    fn assert_type(value: &Value, expected_len: usize, expected_type: TypeCode) {
        let mut output: Vec<u8> = Vec::new();
        let ctype = value.encode(&mut output).unwrap();
        assert_eq!(expected_type, ctype);
        assert_eq!(expected_len, output.len());

        let decoded = decode_value(&mut &output[..]).unwrap();
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
