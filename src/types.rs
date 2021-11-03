/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The types module contains the AMQP 1.0 types system encoders and decoders. By using these types you can enforce a certain encoding for your data.

use std::borrow::Cow;
use std::io::Write;
use std::vec::Vec;

use crate::error::*;
use crate::symbol::Symbol;
use uuid::Uuid;

/**
 * Encoder trait that all types that can be serialized to an AMQP type must implement.
 */
pub trait Encoder {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode>;
}

// Various constants used in encoding and decoding.
pub const U8_MAX: usize = u8::MAX as usize;
pub const I8_MAX: usize = std::i8::MAX as usize;
pub const LIST8_MAX: usize = (u8::MAX as usize) - 1;
pub const LIST32_MAX: usize = (u32::MAX as usize) - 4;

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

/// <http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html#type-sasl-mechanisms>
pub const DESC_SASL_MECHANISMS: Value = Value::Ulong(0x40);
/// <http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html#type-sasl-init>
pub const DESC_SASL_INIT: Value = Value::Ulong(0x41);
/// <http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html#type-sasl-challenge>
pub const DESC_SASL_CHALLENGE: Value = Value::Ulong(0x42);
/// <http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html#type-sasl-response>
pub const DESC_SASL_RESPONSE: Value = Value::Ulong(0x43);
/// <http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html#type-sasl-outcome>
pub const DESC_SASL_OUTCOME: Value = Value::Ulong(0x44);
pub const DESC_ERROR: Value = Value::Ulong(0x1D);

/// Milliseconds since the unix epoch
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct Timestamp(pub u64);

/// A reference to a type with a given value. This allows efficient zero copy of the provided values
/// and should be used when possible (depends on lifetime constraints where its used).
#[derive(Clone, PartialEq, Debug, PartialOrd, Ord, Eq, derive_more::From)]
pub enum ValueRef<'a> {
    Described(Box<ValueRef<'a>>, Box<ValueRef<'a>>),
    Null,
    #[from]
    Bool(&'a bool),
    #[from]
    Ubyte(&'a u8),
    #[from]
    Ushort(&'a u16),
    #[from]
    Uint(&'a u32),
    #[from]
    Ulong(&'a u64),
    #[from]
    Byte(&'a i8),
    #[from]
    Short(&'a i16),
    #[from]
    Int(&'a i32),
    #[from]
    Long(&'a i64),
    // Float(&'a f32),  does not impl Ord nor Eq, but is used as key in *Maps in this crate...
    // Double(&'a f64), does not impl Ord nor Eq, but is used as key in *Maps in this crate...
    // decimal32
    // decimal64
    // decimal128
    #[from]
    Char(&'a char),
    Timestamp(&'a u64),
    Uuid(&'a Uuid),
    #[from]
    Binary(&'a [u8]),
    #[from]
    String(&'a str),
    Symbol(&'a [u8]),
    SymbolRef(&'a str),
    List(&'a Vec<Value>),
    #[from]
    ListRef(&'a Vec<ValueRef<'a>>),
    Map(&'a Vec<(Value, Value)>),
    #[from]
    MapRef(&'a Vec<(ValueRef<'a>, ValueRef<'a>)>),
    Array(&'a Vec<Value>),
    ArrayRef(&'a Vec<ValueRef<'a>>),
}

impl<'a> From<&'a Symbol> for ValueRef<'a> {
    fn from(s: &'a Symbol) -> Self {
        Self::Symbol(&s.0)
    }
}

impl<'a> From<&'a Timestamp> for ValueRef<'a> {
    fn from(t: &'a Timestamp) -> Self {
        Self::Timestamp(&t.0)
    }
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(value: &'a Value) -> Self {
        match value {
            Value::Described(ref descriptor, ref value) => ValueRef::Described(
                Box::new(descriptor.value_ref()),
                Box::new(value.value_ref()),
            ),
            Value::Null => ValueRef::Null,
            Value::Bool(ref value) => ValueRef::Bool(value),
            Value::Str(value) => ValueRef::String(value),
            Value::String(ref value) => ValueRef::String(value),
            Value::Symbol(ref value) => ValueRef::Symbol(&value[..]),
            Value::SymbolSlice(value) => ValueRef::Symbol(&value[..]),
            Value::Ubyte(ref value) => ValueRef::Ubyte(value),
            Value::Ushort(ref value) => ValueRef::Ushort(value),
            Value::Uint(ref value) => ValueRef::Uint(value),
            Value::Ulong(ref value) => ValueRef::Ulong(value),
            Value::Byte(ref value) => ValueRef::Byte(value),
            Value::Short(ref value) => ValueRef::Short(value),
            Value::Int(ref value) => ValueRef::Int(value),
            Value::Long(ref value) => ValueRef::Long(value),
            Value::Array(ref value) => ValueRef::Array(value),
            Value::List(ref value) => ValueRef::List(value),
            Value::Map(ref value) => ValueRef::Map(value),
            Value::Binary(ref value) => ValueRef::Binary(value),
            Value::Char(ref value) => ValueRef::Char(value),
            Value::Timestamp(ref value) => ValueRef::Timestamp(value),
            Value::Uuid(ref value) => ValueRef::Uuid(value),
        }
    }
}

/// An owned value type that can be transferred to or from the broker.
/// <http://docs.oasis-open.org/amqp/core/v1.0/csprd01/amqp-core-types-v1.0-csprd01.html#toc>
#[derive(Clone, PartialEq, Debug, PartialOrd, Ord, Eq, derive_more::From)]
pub enum Value {
    Described(Box<Value>, Box<Value>),
    Null,
    #[from]
    Bool(bool),
    #[from]
    Ubyte(u8),
    #[from]
    Ushort(u16),
    #[from]
    Uint(u32),
    #[from]
    Ulong(u64),
    #[from]
    Byte(i8),
    #[from]
    Short(i16),
    #[from]
    Int(i32),
    #[from]
    Long(i64),
    // Float(f32),  does not impl Ord nor Eq, but is used as key in *Maps in this crate...
    // Double(f64), does not impl Ord nor Eq, but is used as key in *Maps in this crate...
    // decimal32
    // decimal64
    // decimal128
    #[from]
    Char(char),
    Timestamp(u64),
    Uuid(Uuid),
    #[from]
    Binary(Vec<u8>),
    #[from]
    Str(&'static str),
    #[from]
    String(String),
    Symbol(Vec<u8>),
    SymbolSlice(&'static [u8]),
    #[from]
    List(Vec<Value>),
    #[from]
    Map(Vec<(Value, Value)>),
    Array(Vec<Value>),
}

impl Value {
    /**
     * Convert to a reference type. Not currently implemented for all types.
     */
    pub fn value_ref(&self) -> ValueRef {
        ValueRef::from(self)
    }
}

impl From<Symbol> for Value {
    fn from(s: Symbol) -> Self {
        match s.0 {
            Cow::Owned(owned) => Self::Symbol(owned),
            Cow::Borrowed(borrowed) => Self::SymbolSlice(borrowed),
        }
    }
}

impl From<Timestamp> for Value {
    fn from(t: Timestamp) -> Self {
        Self::Timestamp(t.0)
    }
}

impl<V: Into<Value>, const N: usize> From<[V; N]> for Value {
    fn from(array: [V; N]) -> Self {
        Self::Array(
            std::array::IntoIter::new(array)
                .map(Into::into)
                .collect::<Vec<Value>>(),
        )
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
    BooleanTrue = 0x41,
    BooleanFalse = 0x42,
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
    // float
    // double
    // decimal32
    // decimal32
    // decimal12
    Char = 0x73,
    Timestamp = 0x83,
    Uuid = 0x98,
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
