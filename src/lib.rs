/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

mod error;
mod framing;
mod transport;
mod types;

use std::collections::HashMap;
use std::convert::From;
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
    HdrSent,
    HdrExch,
    OpenRcvd,
    OpenSent,
    ClosePipe,
    Opened,
    CloseRcvd,
    CloseSent,
    End,
}

pub struct Link {}

pub struct Sender {}

pub struct Receiver {}

const AMQP_10_VERSION: [u8; 8] = [65, 77, 81, 80, 0, 1, 0, 0];

type Handle = usize;

#[derive(Debug)]
pub struct ConnectionDriver {
    connections: HashMap<Handle, Connection>,
    handles: Vec<Handle>,
    id_counter: usize,
    last_checked: Handle,
}

#[derive(Debug)]
pub struct Connection {
    pub container_id: String,
    pub hostname: String,
    state: ConnectionState,
    transport: transport::Transport,
    opened: bool,
    closed: bool,
    close_condition: Option<ErrorCondition>,

    sessions: HashMap<SessionId, Session>,
}

type SessionId = u32;

#[derive(Debug)]
pub struct Session {
    pub local_id: SessionId,
    remote_id: SessionId,
    begun: bool,
    ended: bool,
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

pub type EventBuffer = Vec<Event>;

#[derive(Debug)]
pub enum Event {
    ConnectionInit,
    RemoteOpen(framing::Open),
    LocalOpen(framing::Open),
    RemoteClose(framing::Close),
    LocalClose(Option<ErrorCondition>),
    /*
    SessionInit(SessionId),
    LocalBegin(SessionId, framing::Begin),
    RemoteBegin(SessionId, framing::Begin),
    LocalEnd(SessionId, framing::End),
    RemoteEnd(SessionId, framing::End),
    */
}

impl ConnectionDriver {
    pub fn new() -> ConnectionDriver {
        ConnectionDriver {
            connections: HashMap::new(),
            handles: Vec::new(),
            id_counter: 0,
            last_checked: 0,
        }
    }

    fn next_handle(self: &mut Self, current: Handle) -> Handle {
        (current + 1) % self.connections.len()
    }

    pub fn register(self: &mut Self, connection: Connection) -> Handle {
        let handle = self.id_counter;
        self.connections.insert(handle, connection);
        self.handles.push(handle);
        self.id_counter += 1;
        handle
    }

    pub fn connection(self: &mut Self, handle: &Handle) -> Option<&mut Connection> {
        self.connections.get_mut(handle)
    }

    // Poll for events on one of the handles registered with this driver and push the events to the provided buffer.
    pub fn poll(self: &mut Self, event_buffer: &mut EventBuffer) -> Result<Option<Handle>> {
        if self.handles.len() > 0 {
            let last: Handle = self.last_checked;
            loop {
                let next = self.next_handle(self.last_checked);

                let conn = self
                    .connections
                    .get_mut(&next)
                    .expect(format!("Handle {:?} missing!", next).as_str());
                let found = conn.poll(event_buffer);
                self.last_checked = next;
                match found {
                    Err(AmqpError::IoError(ref e))
                        if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) => return Err(e),
                    Ok(true) => return Ok(Some(next)),
                    _ => {}
                }
                if next == last {
                    return Ok(None);
                }
            }
        }
        Ok(None)
    }
}

fn unwrap_frame(frame: framing::Frame) -> Result<(u16, framing::Performative)> {
    match frame {
        framing::Frame::AMQP {
            channel: channel,
            performative,
            payload: _,
        } => {
            // Handle AMQP
            let performative = performative.expect("Missing required performative for AMQP frame");

            return Ok((channel, performative));
        }
        _ => return Err(AmqpError::framing_error()),
    }
}

impl Connection {
    pub fn new(container_id: &str, hostname: &str, transport: transport::Transport) -> Connection {
        Connection {
            container_id: container_id.to_string(),
            hostname: hostname.to_string(),
            state: ConnectionState::Start,
            opened: false,
            closed: false,
            sessions: HashMap::new(),
            close_condition: None,
            transport: transport,
        }
    }

    pub fn open(self: &mut Self) {
        self.opened = true;
    }

    pub fn session(self: &mut Self) -> &mut Session {
        let id = 0;
        let s = Session {
            local_id: id,
            remote_id: 0,
            begun: false,
            ended: false,
        };
        self.sessions.insert(id, s);
        self.sessions.get_mut(&id).unwrap()
    }

    pub fn close(self: &mut Self, condition: Option<ErrorCondition>) {
        self.closed = true;
        self.close_condition = condition;
    }

    fn poll(self: &mut Self, event_buffer: &mut EventBuffer) -> Result<bool> {
        let before = event_buffer.len();
        self.do_work(event_buffer)?;

        Ok(before != event_buffer.len())
    }

    fn do_work(self: &mut Self, event_buffer: &mut EventBuffer) -> Result<()> {
        // TODO: Clean up this state handling
        match self.state {
            ConnectionState::Start => {
                self.transport.write(&AMQP_10_VERSION)?;
                self.transport.flush()?;
                self.state = ConnectionState::HdrSent;
            }
            ConnectionState::HdrSent => {
                let mut _remote_version = self.transport.read_protocol_version()?;
                self.state = ConnectionState::HdrExch;
                event_buffer.push(Event::ConnectionInit);
            }
            ConnectionState::HdrExch => {
                if self.opened {
                    self.local_open(event_buffer)?;
                    self.state = ConnectionState::OpenSent;
                } else {
                    let frame = self.transport.read_frame()?;
                    let (_, performative) = unwrap_frame(frame)?;
                    match performative {
                        framing::Performative::Open(open) => {
                            event_buffer.push(Event::RemoteOpen(open));
                            self.state = ConnectionState::OpenRcvd;
                        }
                        _ => return Err(AmqpError::framing_error()),
                    }
                }
            }
            ConnectionState::OpenRcvd => {
                if self.opened {
                    self.local_open(event_buffer)?;
                    self.state = ConnectionState::Opened;
                }
            }
            ConnectionState::OpenSent => {
                if self.closed {
                    self.local_close(event_buffer)?;
                    self.state = ConnectionState::ClosePipe;
                } else {
                    let frame = self.transport.read_frame()?;
                    let (_, performative) = unwrap_frame(frame)?;
                    match performative {
                        framing::Performative::Open(open) => {
                            event_buffer.push(Event::RemoteOpen(open));
                            self.state = ConnectionState::Opened;
                        }
                        framing::Performative::Close(close) => {
                            event_buffer.push(Event::RemoteClose(close));
                            self.state = ConnectionState::ClosePipe;
                        }
                        _ => return Err(AmqpError::framing_error()),
                    }
                }
            }
            ConnectionState::Opened => {
                if self.closed {
                    self.local_close(event_buffer)?;
                    self.state = ConnectionState::CloseSent;
                } else {
                    let frame = self.transport.read_frame()?;
                    self.handle_frame(frame, event_buffer)?;
                }
            }
            ConnectionState::ClosePipe => {
                let frame = self.transport.read_frame()?;
                let (_, performative) = unwrap_frame(frame)?;
                match performative {
                    framing::Performative::Open(open) => {
                        event_buffer.push(Event::RemoteOpen(open));
                        self.state = ConnectionState::CloseSent;
                    }
                    _ => return Err(AmqpError::framing_error()),
                }
            }
            ConnectionState::CloseRcvd => {
                if self.closed {
                    self.local_close(event_buffer)?;
                    self.state = ConnectionState::End;
                }
            }
            ConnectionState::CloseSent => {
                let frame = self.transport.read_frame()?;
                let (_, performative) = unwrap_frame(frame)?;
                match performative {
                    framing::Performative::Close(close) => {
                        event_buffer.push(Event::RemoteClose(close));
                        self.state = ConnectionState::End;
                    }
                    _ => return Err(AmqpError::framing_error()),
                }
            }
            ConnectionState::End => {}
        }
        Ok(())
    }

    // Handle frames in normal operation
    fn handle_frame(
        self: &mut Self,
        frame: framing::Frame,
        event_buffer: &mut EventBuffer,
    ) -> Result<()> {
        let (_, performative) = unwrap_frame(frame)?;
        match performative {
            // TODO: Handle sessions, links etc...
            framing::Performative::Close(close) => {
                event_buffer.push(Event::RemoteClose(close));
                self.state = ConnectionState::CloseRcvd;
            }
            _ => return Err(AmqpError::framing_error()),
        }
        Ok(())
    }

    fn local_open(self: &mut Self, event_buffer: &mut EventBuffer) -> Result<()> {
        let frame = framing::Frame::AMQP {
            channel: 0,
            performative: Some(framing::Performative::Open(framing::Open {
                hostname: self.hostname.clone(),
                ..Default::default()
            })),
            payload: None,
        };

        self.transport.write_frame(&frame)?;
        self.transport.flush()?;

        if let framing::Frame::AMQP {
            channel: _,
            performative,
            payload: _,
        } = frame
        {
            if let Some(framing::Performative::Open(data)) = performative {
                event_buffer.push(Event::LocalOpen(data));
            }
        }
        Ok(())
    }

    fn local_close(self: &mut Self, event_buffer: &mut EventBuffer) -> Result<()> {
        let frame = framing::Frame::AMQP {
            channel: 0,
            performative: Some(framing::Performative::Close(framing::Close {
                error: self.close_condition.clone(),
            })),
            payload: None,
        };

        self.transport.write_frame(&frame)?;
        self.transport.flush()?;

        let condition = self.close_condition.clone();
        event_buffer.push(Event::LocalClose(condition));
        Ok(())
    }
}

impl Session {
    pub fn begin(self: &mut Self) {
        self.begun = true;
    }
}
