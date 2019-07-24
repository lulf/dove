/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use std::error;
use std::fmt;
use std::io;

pub type Result<T> = std::result::Result<T, AmqpError>;

#[derive(Debug)]
pub enum AmqpError {
    IoError(io::Error),
    InternalError(String),
    DecodeError(String),
    NotImplemented,
    Generic(String),
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
            AmqpError::InternalError(s) => write!(f, "{}", s),
            AmqpError::DecodeError(s) => write!(f, "{}", s),
            AmqpError::NotImplemented => write!(f, "Not Implemented"),
            AmqpError::Generic(s) => write!(f, "{}", s),
        }
    }
}

impl std::convert::From<io::Error> for AmqpError {
    fn from(error: io::Error) -> Self {
        return AmqpError::IoError(error);
    }
}

impl std::convert::From<std::string::FromUtf8Error> for AmqpError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        return AmqpError::Generic(error.to_string());
    }
}
