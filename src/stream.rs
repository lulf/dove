use std::{convert::TryInto, io::{Read, Write}, sync::Arc};
use mio::net::TcpStream;
use rustls::{ClientConfig, ClientConnection, StreamOwned};
use core::fmt::{Debug};

use crate::{error::AmqpError, types::Host};
use crate::error::Result;

pub trait Stream: ToTcp 
                + FromTcp 
                + Read 
                + Write 
                + Debug
                + Sync 
                + Send 
                + 'static {}

impl Stream for Rustls {}
impl Stream for TcpStream {}

pub trait ToTcp {
    fn as_ref(&self) -> &TcpStream;
    fn as_mut(&mut self) -> &mut TcpStream;
}

pub trait FromTcp: Sized {
    type C: Send + Clone;
    
    fn wrap(tcp: TcpStream, host: &Host, config: Self::C) -> Result<Self>;
}

// RustLS
#[derive(Debug)]
pub struct Rustls(StreamOwned<ClientConnection, TcpStream>);

impl Rustls {
    fn inner(&self) -> &StreamOwned<ClientConnection, TcpStream> {
        match self {
            Rustls(stream) => stream
        }
    }

    fn inner_mut(&mut self) -> &mut StreamOwned<ClientConnection, TcpStream> {
        match self {
            Rustls(stream) => stream
        }
    }
}

impl ToTcp for Rustls {
    fn as_ref(&self) -> &TcpStream {
        self.inner().get_ref()
    }
    
    fn as_mut(&mut self) -> &mut TcpStream {
        self.inner_mut().get_mut()
    }
}

impl Read for Rustls {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner_mut().read(buf)
    }
}

impl Write for Rustls {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.inner_mut().write(data)
    }
    
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner_mut().flush()
    }
}

// Plain
impl ToTcp for TcpStream {
    fn as_ref(&self) -> &TcpStream {
        self
    }

    fn as_mut(&mut self) -> &mut TcpStream {
        self
    }
}

#[derive(Clone)]
pub struct EmptyConfig;

impl FromTcp for TcpStream {
    type C = EmptyConfig;

    fn wrap(tcp: TcpStream, _host: &Host, _config: EmptyConfig) -> Result<Self> {
        Ok(tcp)
    }
}

impl FromTcp for Rustls {
    type C = ClientConfig; 

    fn wrap(tcp: TcpStream, host: &Host, config: ClientConfig) -> Result<Self> {
        let server_name = 
            match (&host.0 as &str).try_into() {
                Ok(name) => Ok(name),
                Err(err) => Err(AmqpError::Generic(format!("Unable to cast hostname. {:?}", err))),
            };

        let client_connection = 
            match rustls::ClientConnection::new(Arc::new(config), server_name?) {
                Ok(conn) => Ok(conn),
                Err(err) => Err(AmqpError::Generic(format!("Rustls connection failed. {}", err))),
            };
        
        Ok(Rustls(rustls::StreamOwned::new(client_connection?, tcp)))
    }
}