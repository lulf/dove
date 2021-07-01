/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The error module implements all AMQP 1.0 error types that is supported by dove. Conversion from many different error types are supported.

use crate::message::Message;
use async_channel::{RecvError, SendError, TryRecvError, TrySendError};
use std::io;

pub type Result<T> = std::result::Result<T, AmqpError>;

#[derive(thiserror::Error, Debug)]
pub enum AmqpError {
    #[error("IoError: {0:?}")]
    IoError(#[from] io::Error),
    #[error("AmqpError: {0:?}")]
    Amqp(ErrorCondition),
    #[error("GenericError: {0:?}")]
    Generic(String),
    #[error("SendError")]
    SendError,
    #[error("ReceiveError: {0:?}")]
    ReceiveError(TryRecvError),

    #[error("amqp:internal-error")]
    AmqpInternalError,
    #[error("amqp:not-found")]
    AmqpNotFound,
    #[error("amqp:decode-error (description: {0:?})")]
    AmqpDecodeError(Option<String>),
    #[error("amqp:not-implemented")]
    AmqpNotImplemented,
    #[error("amqp:resource-limit-exceeded")]
    AmqpResourceLimitExceeded,

    #[error("amqp:connection:forced")]
    AmqpConnectionForced,
    #[error("amqp:connection:framing-error (description: {0:?})")]
    AmqpConnectionFramingError(Option<String>),
    #[error("amqp:connection:redirect")]
    AmqpConnectionRedirect,

    /// `Message` is 456 bytes large, the second-largest variant
    /// is `ErrorCondition` with 48 bytes, therefore `Message` is boxed
    #[error("The link does not have enough credits to send a message")]
    NotEnoughCreditsToSend(Box<Message>),
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
        AmqpError::AmqpInternalError
    }

    pub fn framing_error(description: Option<&str>) -> AmqpError {
        AmqpError::AmqpConnectionFramingError(description.map(ToString::to_string))
    }

    pub fn not_implemented() -> AmqpError {
        AmqpError::AmqpNotImplemented
    }

    pub fn decode_error(description: Option<&str>) -> AmqpError {
        AmqpError::AmqpDecodeError(description.map(ToString::to_string))
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

impl<T> From<SendError<T>> for AmqpError {
    fn from(_: SendError<T>) -> Self {
        AmqpError::SendError
    }
}

impl<T> From<TrySendError<T>> for AmqpError {
    fn from(_: TrySendError<T>) -> Self {
        AmqpError::SendError
    }
}

impl From<RecvError> for AmqpError {
    fn from(_: RecvError) -> Self {
        AmqpError::ReceiveError(TryRecvError::Closed)
    }
}

impl From<TryRecvError> for AmqpError {
    fn from(err: TryRecvError) -> Self {
        AmqpError::ReceiveError(err)
    }
}
