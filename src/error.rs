/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The error module implements all AMQP 1.0 error types that is supported by dove. Conversion from many different error types are supported.

use crate::message::Message;
use crate::sasl::SaslMechanism;
use async_channel::{RecvError, SendError, TryRecvError, TrySendError};
use std::io;

pub type Result<T> = std::result::Result<T, AmqpError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorCondition {
    pub condition: String,
    pub description: String,
}

impl ErrorCondition {
    pub fn local_idle_timeout() -> Self {
        ErrorCondition {
            condition: AmqpError::AmqpResourceLimitExceeded.to_string(),
            description: "local-idle-timeout expired".to_string(),
        }
    }

    pub fn detach_received() -> Self {
        ErrorCondition {
            condition: "amqp:resource-deleted".to_string(),
            description: "Received detach command - was the link deleted?".to_string(),
        }
    }
}

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
    #[error("The server expected a SASL configuration but none was supplied")]
    SaslConfigurationExpected,
    #[error("Received a Transfer-Frame without payload but expected some")]
    TransferFrameIsMissingPayload,
    #[error("Received a Transfer-Frame without a delivery tag")]
    TransferFrameIsMissingDeliveryTag,
    #[error("Received a Transfer-Frame without a delivery id")]
    TransferFrameIsMissingDeliveryId,
    #[error("Cannot allocate another session because all available ids are already in use")]
    SessionAllocationExhausted,
    #[error("The handle is (no longer) valid")]
    InvalidHandle,
    #[error("The target {0:?} is not recognized by the broker")]
    TargetNotRecognized(String),
    #[error("This client does not support the desired SASL mechanism {0:?}")]
    SaslMechanismNotSupported(SaslMechanism),
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
