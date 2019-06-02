/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use std::io;
use std::error;
use std::net::TcpStream;
use std::fmt;

type Result<T> = std::result::Result<T, AmqpError>;

#[derive(Debug, Clone)]
pub struct AmqpError {
    msg: String,
}

impl error::Error for AmqpError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

impl fmt::Display for AmqpError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg) 
    }
}

impl AmqpError {
    fn new(message: &'static str) -> AmqpError {
        AmqpError {
            msg: String::from(message),
        }
    }
}

pub struct ConnectionOptions {
    host: String,
    port: u16,
}

pub struct Container {
    id: String,
}

pub struct Connection {
    
}

pub struct Session {
    
}

pub struct Link {
    
}

pub struct Sender {
    
}

pub struct Receiver {
    
}

impl Container {
    pub fn new(id: String) -> Container {
        Container {
            id: id,
        }
    }
    pub fn connect(&self, opts: ConnectionOptions) -> io::Result<()> {
        let mut stream = TcpStream::connect(format!("{}:{}", opts.host, opts.port))?;
        return Ok(());
    }
}

impl Connection {
    pub fn createSession() -> Result<Session> {
        return Err(AmqpError::new("Not yet implemented"));
    }

    pub fn createSender(&self, name: String) -> Result<Sender> {
        return Err(AmqpError::new("Not yet implemented"));
    }

    pub fn createReceiver(&self, name: String) -> Result<Receiver> {
        return Err(AmqpError::new("Not yet implemented"));
    }
}

impl Session {
    pub fn createSender(&self, name: String) -> Result<Sender> {
        return Err(AmqpError::new("Not yet implemented"));
    }

    pub fn createReceiver(&self, name: String) -> Result<Receiver> {
        return Err(AmqpError::new("Not yet implemented"));
    }
}
