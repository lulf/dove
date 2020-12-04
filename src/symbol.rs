/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The symbol module contains symbol type specific code.

use std::io::Write;
use std::vec::Vec;

use crate::error::*;
use crate::types::*;

/**
 * A type for working with symbols, typically used in some AMQP structures.
 */
#[derive(Clone, PartialEq, Debug, PartialOrd, Ord, Eq)]
pub struct Symbol {
    data: Vec<u8>,
}

impl Symbol {
    pub fn from_slice(data: &[u8]) -> Symbol {
        let mut vec = Vec::new();
        vec.extend_from_slice(data);
        Symbol { data: vec }
    }

    pub fn symbol_from_str(data: &str) -> Symbol {
        let mut vec = Vec::new();
        vec.extend_from_slice(data.as_bytes());
        Symbol { data: vec }
    }

    pub fn from_vec(data: Vec<u8>) -> Symbol {
        Symbol { data }
    }

    pub fn from_string(data: &str) -> Symbol {
        let mut vec = Vec::new();
        vec.extend_from_slice(data.as_bytes());
        Symbol { data: vec }
    }

    pub fn to_slice(&self) -> &[u8] {
        &self.data[..]
    }
}

impl Encoder for Symbol {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::Symbol(&self.data[..]).encode(writer)
    }
}

impl Encoder for Vec<Symbol> {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut values = Vec::new();
        for sym in self.iter() {
            values.push(ValueRef::Symbol(sym.to_slice()));
        }
        ValueRef::ArrayRef(&values).encode(writer)
    }
}
