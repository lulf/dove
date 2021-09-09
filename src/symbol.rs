/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The symbol module contains symbol type specific code.

use std::io::Write;
use std::vec::Vec;

use crate::error::*;
use crate::types::*;
use std::borrow::Cow;

/**
 * A type for working with symbols, typically used in some AMQP structures.
 */
#[derive(Clone, PartialEq, Debug, PartialOrd, Ord, Eq, derive_more::From)]
pub struct Symbol(#[from(forward)] pub(crate) Cow<'static, [u8]>);

impl Symbol {
    pub fn from_static_str(name: &'static str) -> Self {
        Self(Cow::Borrowed(name.as_bytes()))
    }

    pub fn from_string(name: String) -> Symbol {
        Self::from(name.into_bytes())
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0[..]
    }
}

impl Encoder for Symbol {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::Symbol(&self.0[..]).encode(writer)
    }
}

impl Encoder for Vec<Symbol> {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut values = Vec::new();
        for sym in self.iter() {
            values.push(ValueRef::from(sym));
        }
        ValueRef::ArrayRef(&values).encode(writer)
    }
}
