/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

mod error;
mod framing;
mod transport;
mod types;

use std::collections::BTreeMap;
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
    pub channel_max: u16,
    state: ConnectionState,
    transport: transport::Transport,
    opened: bool,
    closed: bool,
    close_condition: Option<ErrorCondition>,
    sessions: HashMap<ChannelId, Session>,
    remote_channel_map: HashMap<ChannelId, ChannelId>,
}

type ChannelId = u16;

#[derive(Debug)]
enum SessionState {
    Unmapped,
    BeginSent,
    BeginRcvd,
    Mapped,
    EndSent,
    EndRcvd,
    Discarding,
}

#[derive(Debug)]
pub struct Session {
    pub local_channel: ChannelId,
    remote_channel: Option<ChannelId>,
    state: SessionState,
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
    SessionInit(ChannelId),
    LocalBegin(ChannelId, framing::Begin),
    RemoteBegin(ChannelId, framing::Begin),
    /*
    LocalEnd(ChannelId, framing::End),
    RemoteEnd(ChannelId, framing::End),
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

fn unwrap_frame(frame: framing::Frame) -> Result<(ChannelId, framing::Performative)> {
    match frame {
        framing::Frame::AMQP {
            channel: channel,
            body: body,
        } => {
            // Handle AMQP
            let body = body.expect("Missing required performative for AMQP frame");

            return Ok((channel as ChannelId, body));
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
            channel_max: std::u16::MAX,
            opened: false,
            closed: false,
            sessions: HashMap::new(),
            remote_channel_map: HashMap::new(),
            close_condition: None,
            transport: transport,
        }
    }

    pub fn open(self: &mut Self) {
        self.opened = true;
    }

    fn allocate_channel(self: &mut Self) -> Option<ChannelId> {
        for i in 0..self.channel_max {
            let chan = i as ChannelId;
            if !self.sessions.contains_key(&chan) {
                return Some(chan);
            }
        }
        None
    }

    pub fn session(self: &mut Self) -> &mut Session {
        self.session_internal(None)
    }

    fn session_internal(self: &mut Self, channel_id: Option<ChannelId>) -> &mut Session {
        let chan = self.allocate_channel().unwrap();
        let s = Session {
            remote_channel: channel_id,
            local_channel: chan,
            begun: false,
            ended: false,
            state: SessionState::Unmapped,
        };
        self.sessions.insert(chan, s);
        channel_id.map(|c| self.remote_channel_map.insert(c, chan));
        self.sessions.get_mut(&chan).unwrap()
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
                    let (_, body) = unwrap_frame(frame)?;
                    match body {
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
                    self.dispatch_work(event_buffer)?;
                    let frame = self.transport.read_frame()?;
                    self.dispatch_frame(frame, event_buffer)?;
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

    // Dispatch to work performed by sub-endpoints
    fn dispatch_work(self: &mut Self, event_buffer: &mut EventBuffer) -> Result<()> {
        for (channel_id, session) in self.sessions.iter_mut() {
            match session.state {
                SessionState::Unmapped => {
                    if session.begun {
                        let frame = session.local_begin(&mut self.transport, event_buffer)?;
                        session.state = SessionState::BeginSent;
                    }
                }
                SessionState::BeginRcvd => {
                    if session.begun {
                        let frame = session.local_begin(&mut self.transport, event_buffer)?;
                        session.state = SessionState::Mapped;
                    }
                }
                SessionState::BeginSent | SessionState::Mapped => {
                    session.dispatch_work(&mut self.transport, event_buffer)?;
                }
                _ => return Err(AmqpError::not_implemented()),
            }
        }

        /*
        let now = SystemTime::now();
        if self.last_keepalive < self.idle_timeout
        */
        Ok(())
    }

    // Dispatch frame to relevant endpoint
    fn dispatch_frame(
        self: &mut Self,
        frame: framing::Frame,
        event_buffer: &mut EventBuffer,
    ) -> Result<()> {
        let (channel_id, performative) = unwrap_frame(frame)?;

        let mut consumed = self.process_frame(channel_id, &performative, event_buffer)?;
        let local_channel_opt = self.remote_channel_map.get_mut(&channel_id);
        if let Some(local_channel) = local_channel_opt {
            let session = self.sessions.get_mut(&local_channel).unwrap();
            consumed |= session.process_frame(performative, event_buffer)?;
        }
        if !consumed {
            Err(AmqpError::framing_error())
        } else {
            Ok(())
        }
    }

    // Handle frames for a connection
    fn process_frame(
        self: &mut Self,
        channel_id: ChannelId,
        performative: &framing::Performative,
        event_buffer: &mut EventBuffer,
    ) -> Result<bool> {
        Ok(match performative {
            // TODO: Handle sessions, links etc...
            framing::Performative::Begin(begin) => {
                let session = self.session_internal(Some(channel_id));
                session.state = SessionState::BeginRcvd;
                session.remote_channel = Some(channel_id);
                let local_channel = session.local_channel;
                //self.remote_channel_map .insert(channel_id, session.local_channel);
                event_buffer.push(Event::RemoteBegin(session.local_channel, begin.clone()));
                true
            }
            framing::Performative::Close(close) => {
                if channel_id == 0 {
                    event_buffer.push(Event::RemoteClose(close.clone()));
                    self.state = ConnectionState::CloseRcvd;
                    true
                } else {
                    false
                }
            }
            _ => false,
        })
    }

    fn local_open(self: &mut Self, event_buffer: &mut EventBuffer) -> Result<()> {
        let frame = framing::Frame::AMQP {
            channel: 0,
            body: Some(framing::Performative::Open(framing::Open {
                hostname: self.hostname.clone(),
                ..Default::default()
            })),
        };

        self.transport.write_frame(&frame)?;
        self.transport.flush()?;

        if let framing::Frame::AMQP {
            channel: _,
            body: body,
        } = frame
        {
            if let Some(framing::Performative::Open(data)) = body {
                event_buffer.push(Event::LocalOpen(data));
            }
        }
        Ok(())
    }

    fn local_close(self: &mut Self, event_buffer: &mut EventBuffer) -> Result<()> {
        let frame = framing::Frame::AMQP {
            channel: 0,
            body: Some(framing::Performative::Close(framing::Close {
                error: self.close_condition.clone(),
            })),
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

    fn process_frame(
        self: &mut Self,
        performative: framing::Performative,
        event_buffer: &mut EventBuffer,
    ) -> Result<bool> {
        Ok(match self.state {
            SessionState::Unmapped => match performative {
                framing::Performative::Begin(begin) => {
                    self.remote_channel = begin.remote_channel;
                    event_buffer.push(Event::RemoteBegin(self.local_channel, begin));
                    self.state = SessionState::BeginRcvd;
                    true
                }
                _ => false,
            },
            SessionState::BeginSent => match performative {
                framing::Performative::Begin(begin) => {
                    event_buffer.push(Event::RemoteBegin(self.local_channel, begin));
                    self.state = SessionState::Mapped;
                    true
                }
                _ => false,
            },
            _ => false,
        })
    }

    fn local_begin(
        self: &mut Self,
        transport: &mut transport::Transport,
        event_buffer: &mut EventBuffer,
    ) -> Result<()> {
        let frame = framing::Frame::AMQP {
            channel: self.local_channel as u16,
            body: Some(framing::Performative::Begin(framing::Begin {
                remote_channel: self.remote_channel,
                next_outgoing_id: 0,
                incoming_window: 10,
                outgoing_window: 10,
                handle_max: std::u32::MAX,
                offered_capabilities: Vec::new(),
                desired_capabilities: Vec::new(),
                properties: BTreeMap::new(),
            })),
        };

        transport.write_frame(&frame)?;
        transport.flush()?;

        if let framing::Frame::AMQP { channel: _, body } = frame {
            if let Some(framing::Performative::Begin(data)) = body {
                event_buffer.push(Event::LocalBegin(self.local_channel, data));
            }
        }
        Ok(())
    }

    fn dispatch_work(
        self: &mut Self,
        transport: &mut transport::Transport,
        event_buffer: &mut EventBuffer,
    ) -> Result<()> {
        Ok(())
    }
}
