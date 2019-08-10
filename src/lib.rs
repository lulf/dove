/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

mod error;
mod framing;
mod transport;
mod types;

use std::convert::From;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::vec::Vec;

pub use error::Result;
pub use error::*;

#[derive(Debug)]
pub struct ConnectionOptions {
    pub host: &'static str,
    pub port: u16,
}

#[derive(Debug)]
pub struct Container {
    id: String,
}

#[derive(Debug)]
enum ConnectionState {
    Start,
    HdrRcvd,
    HdrSent,
    HdrExch,
    //OpenPipe,
    //OcPipe,
    OpenRcvd,
    OpenSent,
    //ClosePipe,
    Opened,
    CloseRcvd,
    CloseSent,
    //Discarding,
    End,
}

pub struct Session {}

pub struct Link {}

pub struct Sender {}

pub struct Receiver {}

const AMQP_10_VERSION: [u8; 8] = [65, 77, 81, 80, 0, 1, 0, 0];

#[derive(Debug)]
pub struct ConnectionDriver<'a> {
    connection: Connection,
    state: ConnectionState,
    pending_events: Vec<Event<'a>>,
}

#[derive(Debug)]
pub struct Connection {
    pub container_id: String,
    pub hostname: String,
    transport: transport::Transport,
    opened: bool,
    closed: bool,
    close_condition: Option<ErrorCondition>,
}

impl Container {
    pub fn new(id: &'static str) -> Container {
        Container {
            id: String::from(id),
        }
    }

    pub fn connect(&self, opts: ConnectionOptions) -> Result<Connection> {
        let stream = TcpStream::connect(format!("{}:{}", opts.host, opts.port))?;
        // TODO: SASL support
        let transport: transport::Transport = transport::Transport::new(stream, 1024)?;

        Ok(Connection::new(self.id.as_str(), opts.host, transport))
    }

    /*
    pub fn listen(&self, opts: ListenOptions) -> Result<Connection> {
        let stream = TcpStream::connect(format!("{}:{}", opts.host, opts.port))?;
        return Ok(());
    }
    */
}

#[derive(Debug)]
pub enum Event<'a> {
    ConnectionInit(&'a mut Connection),
    RemoteOpen(&'a mut Connection, framing::Open),
    LocalOpen(&'a mut Connection, framing::Open),
    RemoteClose(&'a mut Connection, framing::Close),
    LocalClose(&'a mut Connection, Option<ErrorCondition>),
}

impl<'a> ConnectionDriver<'a> {
    pub fn new(connection: Connection) -> ConnectionDriver<'a> {
        ConnectionDriver {
            pending_events: Vec::new(),
            connection: connection,
            state: ConnectionState::Start,
        }
    }
    // Wait for next state event
    pub fn next_event(self: &mut Self) -> Result<Option<Event>> {
        if self.pending_events.len() > 0 {
            Ok(Some(self.pending_events.remove(0)))
        } else {
            let event = self.process();
            match event {
                Err(AmqpError::IoError(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    Ok(None)
                }
                Err(e) => Err(e),
                Ok(o) => Ok(o),
            }
        }
    }

    fn process(self: &mut Self) -> Result<Option<Event>> {
        // TODO: Clean up this state handling
        match self.state {
            ConnectionState::Start => {
                self.connection.transport.write(&AMQP_10_VERSION)?;
                self.connection.transport.flush()?;
                self.state = ConnectionState::HdrSent;
                Ok(None)
            }
            ConnectionState::HdrSent => {
                let mut remote_version = self.connection.transport.read_protocol_version()?;
                self.state = ConnectionState::HdrExch;
                Ok(Some(Event::ConnectionInit(&mut self.connection)))
            }
            ConnectionState::HdrRcvd => {
                self.connection.transport.write(&AMQP_10_VERSION)?;
                self.connection.transport.flush()?;
                self.state = ConnectionState::HdrExch;
                Ok(Some(Event::ConnectionInit(&mut self.connection)))
            }
            ConnectionState::HdrExch => {
                if self.connection.opened {
                    self.local_open(ConnectionState::OpenSent)
                } else {
                    let frame = self.connection.transport.read_frame()?;
                    if let Some(f) = frame {
                        self.remote_open(f, ConnectionState::OpenRcvd)
                    } else {
                        Ok(None)
                    }
                }
            }
            ConnectionState::OpenRcvd => {
                if self.connection.opened {
                    self.local_open(ConnectionState::Opened)
                } else {
                    Ok(None)
                }
            }
            ConnectionState::OpenSent => {
                let frame = self.connection.transport.read_frame()?;
                if let Some(f) = frame {
                    self.remote_open(f, ConnectionState::Opened)
                } else {
                    Ok(None)
                }
            }
            ConnectionState::Opened => {
                if self.connection.closed {
                    self.local_close(ConnectionState::CloseSent)
                } else {
                    let frame = self.connection.transport.read_frame()?;
                    if let Some(f) = frame {
                        self.handle_frame(f)
                    } else {
                        Ok(None)
                    }
                }
            }
            ConnectionState::CloseRcvd => {
                if self.connection.closed {
                    self.local_close(ConnectionState::End)
                } else {
                    Ok(None)
                }
            }
            ConnectionState::CloseSent => {
                let frame = self.connection.transport.read_frame()?;
                if let Some(f) = frame {
                    self.handle_frame(f)
                } else {
                    Ok(None)
                }
            }
            ConnectionState::End => Ok(None),
            _ => Err(AmqpError::amqp_error(
                error::condition::NOT_IMPLEMENTED,
                None,
            )),
        }
    }

    fn remote_open(
        self: &mut Self,
        frame: framing::Frame,
        next_state: ConnectionState,
    ) -> Result<Option<Event>> {
        match frame {
            framing::Frame::AMQP {
                channel,
                performative,
                payload,
            } => {
                // Handle AMQP
                let performative =
                    performative.expect("Missing required performative for AMQP frame");
                match performative {
                    framing::Performative::Open(open) => {
                        self.state = next_state;
                        return Ok(Some(Event::RemoteOpen(&mut self.connection, open)));
                    }
                    _ => Err(AmqpError::amqp_error(
                        error::condition::connection::FRAMING_ERROR,
                        None,
                    )),
                }
            }
            _ => {
                return Err(AmqpError::amqp_error(
                    error::condition::connection::FRAMING_ERROR,
                    None,
                ))
            }
        }
    }

    fn local_open(self: &mut Self, next_state: ConnectionState) -> Result<Option<Event>> {
        let frame = framing::Frame::AMQP {
            channel: 0,
            performative: Some(framing::Performative::Open(framing::Open {
                hostname: self.connection.hostname.clone(),
                ..Default::default()
            })),
            payload: None,
        };

        self.connection.transport.write_frame(&frame)?;
        self.connection.transport.flush()?;

        self.state = next_state;
        if let framing::Frame::AMQP {
            channel,
            performative,
            payload,
        } = frame
        {
            if let Some(framing::Performative::Open(data)) = performative {
                return Ok(Some(Event::LocalOpen(&mut self.connection, data)));
            }
        }
        Ok(None)
    }

    fn local_close(self: &mut Self, next_state: ConnectionState) -> Result<Option<Event>> {
        let frame = framing::Frame::AMQP {
            channel: 0,
            performative: Some(framing::Performative::Close(framing::Close {
                error: self.connection.close_condition.clone(),
            })),
            payload: None,
        };

        self.connection.transport.write_frame(&frame)?;
        self.connection.transport.flush()?;

        self.state = next_state;

        let condition = self.connection.close_condition.clone();
        Ok(Some(Event::LocalClose(&mut self.connection, condition)))
    }

    fn handle_frame(self: &mut Self, frame: framing::Frame) -> Result<Option<Event>> {
        match frame {
            framing::Frame::AMQP {
                channel,
                performative,
                payload,
            } => {
                // Handle AMQP
                let performative =
                    performative.expect("Missing required performative for AMQP frame");
                match performative {
                    framing::Performative::Close(close) => {
                        self.state = ConnectionState::CloseRcvd;
                        Ok(Some(Event::RemoteClose(&mut self.connection, close)))
                    }
                    _ => Err(AmqpError::amqp_error(
                        error::condition::connection::FRAMING_ERROR,
                        None,
                    )),
                }
            }
            _ => {
                return Err(AmqpError::amqp_error(
                    error::condition::connection::FRAMING_ERROR,
                    None,
                ))
            }
        }
    }
}

impl Connection {
    pub fn new(container_id: &str, hostname: &str, transport: transport::Transport) -> Connection {
        Connection {
            container_id: container_id.to_string(),
            hostname: hostname.to_string(),
            opened: false,
            closed: false,
            close_condition: None,
            transport: transport,
        }
    }

    pub fn open(self: &mut Self) {
        self.opened = true;
    }

    pub fn close(self: &mut Self, condition: Option<ErrorCondition>) {
        self.closed = true;
        self.close_condition = condition;
    }
}
