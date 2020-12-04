/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The error module implements all AMQP 1.0 error types that is supported by dove. Conversion from many different error types are supported.

use std::error;
use std::fmt;
use std::io;

pub type Result<T> = std::result::Result<T, AmqpError>;

#[derive(Debug)]
pub enum AmqpError {
    IoError(io::Error),
    Amqp(ErrorCondition),
    Generic(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorCondition {
    pub condition: String,
    pub description: String,
}

pub mod condition {
    pub const INTERNAL_ERROR: &str = "amqp:internal-error";
    pub const NOT_FOUND: &str = "amqp:not-found";
    pub const DECODE_ERROR: &str = "amqp:decode-error";
    pub const NOT_IMPLEMENTED: &str = "amqp:not-implemented";
    pub const RESOURCE_LIMIT_EXCEEDED: &str = "amqp:resource-limit-exceeded";

    pub mod connection {
        pub const CONNECTION_FORCED: &str = "amqp:connection:forced";
        pub const FRAMING_ERROR: &str = "amqp:connection:framing-error";
        pub const REDIRECT: &str = "amqp:connection:redirect";
    }
}

impl AmqpError {
    pub fn generic(s: &str) -> AmqpError {
        AmqpError::Generic(s.to_string())
    }
    pub fn internal_error() -> AmqpError {
        AmqpError::amqp_error(condition::INTERNAL_ERROR, None)
    }

    pub fn framing_error() -> AmqpError {
        AmqpError::amqp_error(condition::connection::FRAMING_ERROR, None)
    }

    pub fn not_implemented() -> AmqpError {
        AmqpError::amqp_error(condition::NOT_IMPLEMENTED, None)
    }

    pub fn decode_error(description: Option<&str>) -> AmqpError {
        AmqpError::amqp_error(condition::DECODE_ERROR, description)
    }

    pub fn amqp_error(condition: &'static str, description: Option<&str>) -> AmqpError {
        AmqpError::Amqp(ErrorCondition {
            condition: condition.to_string(),
            description: description.unwrap_or("").to_string(),
        })
    }
}

impl error::Error for AmqpError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

impl fmt::Display for AmqpError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AmqpError::IoError(e) => write!(f, "{}", e.to_string()),
            AmqpError::Amqp(s) => write!(f, "{:?}", s),
            AmqpError::Generic(s) => write!(f, "{}", s),
        }
    }
}

impl std::convert::From<io::Error> for AmqpError {
    fn from(error: io::Error) -> Self {
        AmqpError::IoError(error)
    }
}

impl<T> std::convert::From<std::sync::mpsc::SendError<T>> for AmqpError {
    fn from(error: std::sync::mpsc::SendError<T>) -> Self {
        AmqpError::Generic(error.to_string())
    }
}

impl std::convert::From<std::sync::mpsc::RecvError> for AmqpError {
    fn from(error: std::sync::mpsc::RecvError) -> Self {
        AmqpError::Generic(error.to_string())
    }
}

impl std::convert::From<std::sync::mpsc::TryRecvError> for AmqpError {
    fn from(error: std::sync::mpsc::TryRecvError) -> Self {
        AmqpError::Generic(error.to_string())
    }
}

impl std::convert::From<std::str::Utf8Error> for AmqpError {
    fn from(error: std::str::Utf8Error) -> Self {
        AmqpError::Generic(error.to_string())
    }
}

impl std::convert::From<std::string::FromUtf8Error> for AmqpError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        AmqpError::Generic(error.to_string())
    }
}

impl std::convert::From<std::boxed::Box<dyn std::any::Any + std::marker::Send>> for AmqpError {
    fn from(_error: std::boxed::Box<dyn std::any::Any + std::marker::Send>) -> Self {
        AmqpError::Generic("thread error".to_string())
    }
}

impl std::convert::From<std::num::ParseIntError> for AmqpError {
    fn from(error: std::num::ParseIntError) -> Self {
        AmqpError::Generic(error.to_string())
    }
}
