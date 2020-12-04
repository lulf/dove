/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! Conversion functions for types that is used in this implementation. This is used  *
//! when decoding frames. At present there is a lot of duplication, and this part     *
//! could use some refactoring to simplify.                                           *

use std::collections::BTreeMap;
use std::vec::Vec;

use crate::error::*;
use crate::frame_codec::*;
use crate::symbol::*;
use crate::types::*;

pub trait TryFromValue: std::fmt::Debug {
    fn try_from(value: Value) -> Result<Self>
    where
        Self: std::marker::Sized;
}

pub trait TryFromValueVec: TryFromValue {}

impl<T: TryFromValue> TryFromValue for Option<T> {
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Null => Ok(None),
            _ => Ok(Some(T::try_from(value)?)),
        }
    }
}

impl TryFromValue for Vec<u8> {
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Binary(v) => {
                Ok(v)
            }
            v => Err(AmqpError::decode_error(Some(
                format!("Error converting value to u8: {:?}", v).as_str(),
            ))),
        }
    }
}

impl<T: TryFromValueVec> TryFromValue for Vec<T> {
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::List(v) => {
                let (results, errors): (Vec<_>, Vec<_>) = v
                    .into_iter()
                    .map(T::try_from)
                    .partition(Result::is_ok);
                if !errors.is_empty() {
                    Err(AmqpError::decode_error(Some(
                        "Error decoding list elements",
                    )))
                } else {
                    Ok(results.into_iter().map(Result::unwrap).collect())
                }
            }
            Value::Array(v) => {
                let (results, errors): (Vec<_>, Vec<_>) = v
                    .into_iter()
                    .map(T::try_from)
                    .partition(Result::is_ok);
                if !errors.is_empty() {
                    Err(AmqpError::decode_error(Some(
                        format!("Error decoding array elements: {:?}", errors).as_str(),
                    )))
                } else {
                    Ok(results.into_iter().map(Result::unwrap).collect())
                }
            }
            _ => Ok(vec![T::try_from(value)?])
        }
    }
}

impl TryFromValue for u8 {
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Ubyte(v) => Ok(v),
            v => Err(AmqpError::decode_error(Some(
                format!("Error converting value to u8: {:?}", v).as_str(),
            ))),
        }
    }
}

impl TryFromValue for u64 {
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Ulong(v) => Ok(v),
            _ => Err(AmqpError::decode_error(Some(
                "Error converting value to u64",
            ))),
        }
    }
}

impl TryFromValue for u32 {
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Uint(v) => Ok(v),
            _ => Err(AmqpError::amqp_error(
                condition::DECODE_ERROR,
                Some("Error converting value to u32"),
            )),
        }
    }
}

impl TryFromValue for u16 {
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Ushort(v) => Ok(v),
            _ => Err(AmqpError::amqp_error(
                condition::DECODE_ERROR,
                Some("Error converting value to u32"),
            )),
        }
    }
}

impl TryFromValue for bool {
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Bool(v) => Ok(v),
            _ => Err(AmqpError::decode_error(Some(
                "Error converting value to bool",
            ))),
        }
    }
}

impl TryFromValueVec for String {}
impl TryFromValue for String {
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Symbol(v) => Ok(String::from_utf8_lossy(&v[..]).to_string()),
            Value::String(v) => Ok(v),
            _ => Err(AmqpError::amqp_error(
                condition::DECODE_ERROR,
                Some("Error converting value to String"),
            )),
        }
    }
}

impl<T: TryFromValue + std::cmp::Ord> TryFromValue for BTreeMap<T, Value> {
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Map(v) => {
                let mut m = BTreeMap::new();
                for (key, value) in v.into_iter() {
                    m.insert(T::try_from(key)?, value);
                }
                Ok(m)
            }
            _ => Err(AmqpError::decode_error(Some(
                "Error converting value to Map<T, Value>",
            ))),
        }
    }
}

impl TryFromValue for Value {
    fn try_from(value: Value) -> Result<Self> {
        Ok(value)
    }
}

impl TryFromValueVec for Symbol {}
impl TryFromValue for Symbol {
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Symbol(v) => Ok(Symbol::from_vec(v)),
            _ => Err(AmqpError::amqp_error(
                condition::DECODE_ERROR,
                Some("Error converting value to Symbol"),
            )),
        }
    }
}

impl TryFromValue for ErrorCondition {
    fn try_from(value: Value) -> Result<Self> {
        if let Value::Described(descriptor, mut list) = value {
            let decoder = FrameDecoder::new(&descriptor, &mut list)?;
            match *descriptor {
                DESC_ERROR => ErrorCondition::decode(decoder),
                _ => Err(AmqpError::decode_error(Some(
                    format!("Expected error descriptor but found {:?}", *descriptor).as_str(),
                ))),
            }
        } else {
            Err(AmqpError::decode_error(Some(
                "Missing expected error descriptor",
            )))
        }
    }
}
