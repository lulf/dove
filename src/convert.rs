/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use std::collections::BTreeMap;
use std::vec::Vec;

use crate::types::*;
use crate::error::*;
use crate::symbol::*;
use crate::frame_codec::*;

/**
 *************************************************************************************
 * Conversion functions for types that is used in this implementation. This is used  *
 * when decoding frames. At present there is a lot of duplication, and this part     *
 * could use some refactoring to simplify.                                           *
 *************************************************************************************
 */

impl std::convert::TryFrom<Value> for u8 {
    type Error = AmqpError;
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Ubyte(v) => return Ok(v),
            _ => Err(AmqpError::amqp_error(
                condition::DECODE_ERROR,
                Some("Error converting value to u8"),
            )),
        }
    }
}

impl std::convert::TryFrom<Value> for Vec<u8> {
    type Error = AmqpError;
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Binary(v) => return Ok(v),
            _ => Err(AmqpError::amqp_error(
                condition::DECODE_ERROR,
                Some("Error converting value to u8"),
            )),
        }
    }
}

impl std::convert::TryFrom<Value> for Option<Vec<u8>> {
    type Error = AmqpError;
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Null => Ok(None),
            v => Ok(Some(Vec::try_from(v)?)),
        }
    }
}

impl std::convert::TryFrom<Value> for u32 {
    type Error = AmqpError;
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Uint(v) => return Ok(v),
            _ => Err(AmqpError::amqp_error(
                condition::DECODE_ERROR,
                Some("Error converting value to u32"),
            )),
        }
    }
}

impl std::convert::TryFrom<Value> for Option<u32> {
    type Error = AmqpError;
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Null => Ok(None),
            v => Ok(Some(u32::try_from(v)?)),
        }
    }
}

impl std::convert::TryFrom<Value> for u16 {
    type Error = AmqpError;
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Ushort(v) => return Ok(v),
            _ => Err(AmqpError::amqp_error(
                condition::DECODE_ERROR,
                Some("Error converting value to u32"),
            )),
        }
    }
}

impl std::convert::TryFrom<Value> for Option<u16> {
    type Error = AmqpError;
    fn try_from(value: Value) -> Result<Self> {
        Ok(match value {
            Value::Null => None,
            v => Some(u16::try_from(v)?),
        })
    }
}

impl std::convert::TryFrom<Value> for String {
    type Error = AmqpError;
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

impl std::convert::TryFrom<Value> for Option<String> {
    type Error = AmqpError;
    fn try_from(value: Value) -> Result<Self> {
        Ok(match value {
            Value::Null => None,
            v => Some(String::try_from(v)?),
        })
    }
}

impl std::convert::TryFrom<Value> for BTreeMap<String, Value> {
    type Error = AmqpError;
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Map(v) => {
                let mut m = BTreeMap::new();
                for (key, value) in v.into_iter() {
                    m.insert(String::try_from(key)?, value);
                }
                Ok(m)
            }
            _ => Err(AmqpError::amqp_error(
                condition::DECODE_ERROR,
                Some("Error converting value to Vec<Symbol>"),
            )),
        }
    }
}

impl std::convert::TryFrom<Value> for Option<BTreeMap<String, Value>> {
    type Error = AmqpError;
    fn try_from(value: Value) -> Result<Self> {
        Ok(match value {
            Value::Null => None,
            v => Some(BTreeMap::try_from(v)?),
        })
    }
}



impl std::convert::TryFrom<Value> for Symbol {
    type Error = AmqpError;
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

impl std::convert::TryFrom<Value> for Option<Symbol> {
    type Error = AmqpError;
    fn try_from(value: Value) -> Result<Self> {
        Ok(match value {
            Value::Null => None,
            v => Some(Symbol::try_from(v)?),
        })
    }
}

impl std::convert::TryFrom<Value> for Vec<Symbol> {
    type Error = AmqpError;
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Symbol(s) => return Ok(vec![Symbol::from_vec(s)]),
            Value::Array(v) => {
                let (results, errors): (Vec<_>, Vec<_>) = v
                    .into_iter()
                    .map(|f| Symbol::try_from(f))
                    .partition(Result::is_ok);
                if errors.len() > 0 {
                    return Err(AmqpError::amqp_error(
                        condition::DECODE_ERROR,
                        Some("Error decoding some elements"),
                    ));
                } else {
                    return Ok(results.into_iter().map(Result::unwrap).collect());
                }
            }
            Value::List(v) => {
                let (results, errors): (Vec<_>, Vec<_>) = v
                    .into_iter()
                    .map(|f| Symbol::try_from(f))
                    .partition(Result::is_ok);
                if errors.len() > 0 {
                    return Err(AmqpError::amqp_error(
                        condition::DECODE_ERROR,
                        Some("Error decoding some elements"),
                    ));
                } else {
                    return Ok(results.into_iter().map(Result::unwrap).collect());
                }
            }
            _ => Err(AmqpError::amqp_error(
                condition::DECODE_ERROR,
                Some("Error converting value to Vec<Symbol>"),
            )),
        }
    }
}

impl std::convert::TryFrom<Value> for Option<Vec<Symbol>> {
    type Error = AmqpError;
    fn try_from(value: Value) -> Result<Self> {
        Ok(match value {
            Value::Null => None,
            v => Some(Vec::try_from(v)?),
        })
    }
}

impl std::convert::TryFrom<Value> for ErrorCondition {
    type Error = AmqpError;
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

impl std::convert::TryFrom<Value> for Option<ErrorCondition> {
    type Error = AmqpError;
    fn try_from(value: Value) -> Result<Self> {
        Ok(match value {
            Value::Null => None,
            v => Some(ErrorCondition::try_from(v)?),
        })
    }
}

