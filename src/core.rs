/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use log::trace;
use rand::Rng;
use std::collections::HashMap;
use std::net::TcpListener;
use std::net::TcpStream;
use std::time::Duration;
use std::time::Instant;
use std::vec::Vec;

use crate::error::*;
use crate::framing::*;
use crate::message::*;
use crate::sasl::*;
use crate::transport::*;
use crate::types::*;

#[derive(Debug)]
pub struct ConnectionOptions<'a> {
    pub container_id: &'a str,
    pub username: Option<String>,
    pub password: Option<String>,
    pub sasl_mechanism: Option<SaslMechanism>,
}

impl<'a> ConnectionOptions<'a> {
    pub fn new(container_id: &'a str) -> ConnectionOptions {
        ConnectionOptions {
            container_id: container_id,
            username: None,
            password: None,
            sasl_mechanism: None,
        }
    }
}

#[derive(Debug)]
pub struct ListenOptions<'a> {
    pub container_id: &'a str,
}

#[derive(Debug)]
pub struct Container {
    id: String,
}

type ConnectionId = usize;

#[derive(Debug)]
enum ConnectionState {
    Start,
    StartWait,
    HdrSent,
    Sasl,
    HdrExch,
    OpenRcvd,
    OpenSent,
    ClosePipe,
    Opened,
    CloseRcvd,
    CloseSent,
    End,
}

const AMQP_10_HEADER: ProtocolHeader = ProtocolHeader::AMQP(Version(1, 0, 0));
const SASL_10_HEADER: ProtocolHeader = ProtocolHeader::SASL(Version(1, 0, 0));

#[derive(Debug)]
pub struct ConnectionDriver {
    connections: HashMap<ConnectionId, Connection>,
}

#[derive(Debug)]
pub struct Connection {
    pub id: ConnectionId,
    pub container_id: String,
    pub hostname: String,
    pub channel_max: u16,
    pub idle_timeout: Duration,
    pub remote_idle_timeout: Duration,
    pub remote_container_id: String,
    pub remote_channel_max: u16,
    sasl: Option<Sasl>,
    state: ConnectionState,
    transport: Transport,
    opened: bool,
    closed: bool,
    close_condition: Option<ErrorCondition>,
    sessions: HashMap<ChannelId, Session>,
    remote_channel_map: HashMap<ChannelId, ChannelId>,
    tx_frames: Vec<Frame>,
}

type ChannelId = u16;

type HandleId = u32;

#[allow(dead_code)]
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

#[allow(dead_code)]
#[derive(Debug)]
enum LinkState {
    Unmapped,
    AttachSent,
    AttachRcvd,
    Mapped,
    DetachSent,
    DetachRcvd,
    Discarding,
}

#[derive(Debug)]
pub struct Session {
    pub local_channel: ChannelId,
    remote_channel: Option<ChannelId>,
    handle_max: u32,
    state: SessionState,
    opened: bool,
    closed: bool,
    links: HashMap<HandleId, Link>,
    next_outgoing_id: u32,
}

#[derive(Debug)]
pub struct Link {
    name: String,
    handle: HandleId,
    opened: bool,
    pub role: LinkRole,
    source: Option<Source>,
    target: Option<Target>,
    state: LinkState,
    next_message_id: u64,
    tx: Vec<Delivery>,
    flow: Vec<u32>,
}

#[derive(Debug)]
pub struct Delivery {
    pub message: Message,
    remotely_settled: bool,
    settled: bool,
    state: Option<DeliveryState>,
    tag: Vec<u8>,
}

pub fn connect(
    id: ConnectionId,
    host: &str,
    port: u16,
    opts: ConnectionOptions,
) -> Result<Connection> {
    let stream = TcpStream::connect(format!("{}:{}", host, port))?;
    // TODO: SASL support
    let transport: Transport = Transport::new(stream, 1024)?;

    let mut connection = Connection::new(id, opts.container_id, host, transport);
    if opts.username.is_some() || opts.password.is_some() || opts.sasl_mechanism.is_some() {
        connection.sasl = Some(Sasl {
            role: SaslRole::Client(SaslClient {
                mechanism: opts.sasl_mechanism.unwrap_or(SaslMechanism::Plain),
                username: opts.username,
                password: opts.password,
            }),
            state: SaslState::InProgress,
        });
    }

    Ok(connection)
}

pub struct Listener {
    pub listener: TcpListener,
    pub container_id: String,
    pub sasl_mechanisms: Option<Vec<SaslMechanism>>,
    id_counter: usize,
}

pub fn listen(host: &str, port: u16, opts: ListenOptions) -> Result<Listener> {
    let listener = TcpListener::bind(format!("{}:{}", host, port))?;
    Ok(Listener {
        listener: listener,
        container_id: opts.container_id.to_string(),
        sasl_mechanisms: None,
        id_counter: 0,
    })
}

impl Listener {
    pub fn accept(&mut self) -> Result<Connection> {
        let (stream, addr) = self.listener.accept()?;
        let transport: Transport = Transport::new(stream, 1024)?;

        let id = self.id_counter;
        self.id_counter += 1;

        let mut connection = Connection::new(
            id,
            self.container_id.as_str(),
            addr.ip().to_string().as_str(),
            transport,
        );
        connection.state = ConnectionState::StartWait;
        Ok(connection)
    }
}

pub type EventBuffer = Vec<Event>;

#[derive(Debug)]
pub enum Event {
    ConnectionInit(ConnectionId),
    RemoteOpen(ConnectionId, Open),
    LocalOpen(ConnectionId, Open),
    RemoteClose(ConnectionId, Close),
    LocalClose(ConnectionId, Option<ErrorCondition>),
    SessionInit(ConnectionId, ChannelId),
    LocalBegin(ConnectionId, ChannelId, Begin),
    RemoteBegin(ConnectionId, ChannelId, Begin),
    LocalAttach(ConnectionId, ChannelId, HandleId, Attach),
    RemoteAttach(ConnectionId, ChannelId, HandleId, Attach),
    Flow(ConnectionId, ChannelId, HandleId, Flow),
    Disposition(ConnectionId, ChannelId, Disposition),
    Delivery(ConnectionId, ChannelId, HandleId, Delivery),
    /*
    LocalEnd(ChannelId, End),
    RemoteEnd(ChannelId, End),
    */
}

impl ConnectionDriver {
    pub fn new() -> ConnectionDriver {
        ConnectionDriver {
            connections: HashMap::new(),
        }
    }

    /// Register a new connection to be managed by this driver.
    /// # Examples
    /// use dove::core::ConnectionDriver
    /// let connection = connect("localhost:5672")?;
    /// let driver = ConnectionDriver::new();
    /// let handle = driver.register(connection);
    pub fn register(self: &mut Self, connection: Connection) {
        self.connections.insert(connection.id, connection);
    }

    pub fn connection(self: &mut Self, handle: ConnectionId) -> Option<&mut Connection> {
        self.connections.get_mut(&handle)
    }

    // Poll for events on one of the handles registered with this driver and push the events to the provided buffer.
    pub fn poll(self: &mut Self, event_buffer: &mut EventBuffer) -> Result<()> {
        for (_id, conn) in self.connections.iter_mut() {
            let found = conn.poll(event_buffer);
            match found {
                Err(AmqpError::IoError(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
                _ => {}
            }
        }
        Ok(())
    }
}

fn unwrap_frame(frame: Frame) -> Result<(ChannelId, Option<Performative>, Option<Vec<u8>>)> {
    match frame {
        Frame::AMQP(AmqpFrame {
            channel,
            performative,
            payload,
        }) => {
            return Ok((channel as ChannelId, performative, payload));
        }
        _ => return Err(AmqpError::framing_error()),
    }
}

impl Connection {
    pub fn new(
        id: ConnectionId,
        container_id: &str,
        hostname: &str,
        transport: Transport,
    ) -> Connection {
        Connection {
            id: id,
            container_id: container_id.to_string(),
            hostname: hostname.to_string(),
            idle_timeout: Duration::from_millis(5000),
            channel_max: std::u16::MAX,
            remote_container_id: String::new(),
            remote_channel_max: 0,
            remote_idle_timeout: Duration::from_millis(0),
            state: ConnectionState::Start,
            opened: false,
            closed: false,
            sessions: HashMap::new(),
            remote_channel_map: HashMap::new(),
            close_condition: None,
            transport: transport,
            sasl: None,
            tx_frames: Vec::new(),
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

    pub fn create_session(self: &mut Self) -> &mut Session {
        self.session_internal(None)
    }

    pub fn get_session(self: &mut Self, channel_id: ChannelId) -> Option<&mut Session> {
        self.sessions.get_mut(&channel_id)
    }

    fn session_internal(self: &mut Self, channel_id: Option<ChannelId>) -> &mut Session {
        let chan = self.allocate_channel().unwrap();
        let s = Session {
            remote_channel: channel_id,
            local_channel: chan,
            handle_max: std::u32::MAX,
            opened: false,
            closed: false,
            next_outgoing_id: 0,
            state: SessionState::Unmapped,
            links: HashMap::new(),
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

    fn skip_sasl(self: &Self) -> bool {
        self.sasl.is_none() || self.sasl.as_ref().unwrap().is_done()
    }

    fn do_work(self: &mut Self, event_buffer: &mut EventBuffer) -> Result<()> {
        match self.state {
            ConnectionState::StartWait => {
                let header = self.transport.read_protocol_header()?;
                if let Some(header) = header {
                    match header {
                        SASL_10_HEADER if self.sasl.is_some() => {
                            self.transport.write_protocol_header(&SASL_10_HEADER)?;
                            self.state = ConnectionState::Sasl;
                        }
                        AMQP_10_HEADER if self.skip_sasl() => {
                            self.transport.write_protocol_header(&AMQP_10_HEADER)?;
                            self.state = ConnectionState::HdrExch;
                            event_buffer.push(Event::ConnectionInit(self.id));
                        }
                        _ => {
                            self.transport.write_protocol_header(&AMQP_10_HEADER)?;
                            self.transport.close()?;
                            self.state = ConnectionState::End;
                        }
                    }
                }
            }
            ConnectionState::Start => {
                if self.skip_sasl() {
                    self.transport.write_protocol_header(&AMQP_10_HEADER)?;
                } else {
                    self.transport.write_protocol_header(&SASL_10_HEADER)?;
                }
                self.state = ConnectionState::HdrSent;
            }
            ConnectionState::HdrSent => {
                let header = self.transport.read_protocol_header()?;
                if let Some(header) = header {
                    match header {
                        SASL_10_HEADER if self.sasl.is_some() => {
                            self.state = ConnectionState::Sasl;
                        }
                        AMQP_10_HEADER if self.skip_sasl() => {
                            self.state = ConnectionState::HdrExch;
                            event_buffer.push(Event::ConnectionInit(self.id));
                        }
                        _ => {
                            // self.transport.write_protocol_header(&AMQP_10_HEADER)?;
                            self.transport.close()?;
                            self.state = ConnectionState::End;
                        }
                    }
                }
            }
            ConnectionState::Sasl => {
                let sasl = self.sasl.as_mut().unwrap();
                sasl.perform_handshake(&mut self.transport)?;
                match sasl.state {
                    SaslState::Success => {
                        self.state = ConnectionState::Start;
                    }
                    SaslState::Failed => {
                        self.transport.close()?;
                        self.state = ConnectionState::End;
                    }
                    _ => {}
                }
            }
            ConnectionState::HdrExch => {
                if self.opened {
                    self.local_open(event_buffer);
                    self.state = ConnectionState::OpenSent;
                } else {
                    let frame = self.transport.read_frame()?;
                    let (_, performative, _) = unwrap_frame(frame)?;
                    if let Some(performative) = performative {
                        match performative {
                            Performative::Open(open) => {
                                self.update_connection_info(&open);
                                event_buffer.push(Event::RemoteOpen(self.id, open));
                                self.state = ConnectionState::OpenRcvd;
                            }
                            _ => return Err(AmqpError::framing_error()),
                        }
                    }
                }
            }
            ConnectionState::OpenRcvd => {
                if self.opened {
                    self.local_open(event_buffer);
                    self.state = ConnectionState::Opened;
                }
            }
            ConnectionState::OpenSent => {
                if self.closed {
                    self.local_close(event_buffer);
                    self.state = ConnectionState::ClosePipe;
                } else {
                    let frame = self.transport.read_frame()?;
                    let (_, performative, _) = unwrap_frame(frame)?;
                    if let Some(performative) = performative {
                        match performative {
                            Performative::Open(open) => {
                                self.update_connection_info(&open);
                                event_buffer.push(Event::RemoteOpen(self.id, open));
                                self.state = ConnectionState::Opened;
                            }
                            Performative::Close(close) => {
                                event_buffer.push(Event::RemoteClose(self.id, close));
                                self.state = ConnectionState::ClosePipe;
                            }
                            _ => return Err(AmqpError::framing_error()),
                        }
                    }
                }
            }
            ConnectionState::Opened => {
                if self.closed {
                    self.local_close(event_buffer);
                    self.state = ConnectionState::CloseSent;
                } else {
                    self.process_work(event_buffer)?;
                    self.keepalive(event_buffer);

                    // Flush in case we are going to be blocked.
                    self.flush()?;

                    // TODO: Read multiple frames and process
                    let frame = self.transport.read_frame()?;
                    self.process_frame(frame, event_buffer)?;
                }
            }
            ConnectionState::ClosePipe => {
                let frame = self.transport.read_frame()?;
                let (_, performative, _) = unwrap_frame(frame)?;
                if let Some(performative) = performative {
                    match performative {
                        Performative::Open(open) => {
                            event_buffer.push(Event::RemoteOpen(self.id, open));
                            self.state = ConnectionState::CloseSent;
                        }
                        _ => return Err(AmqpError::framing_error()),
                    }
                }
            }
            ConnectionState::CloseRcvd => {
                if self.closed {
                    self.local_close(event_buffer);
                    self.state = ConnectionState::End;
                }
            }
            ConnectionState::CloseSent => {
                let frame = self.transport.read_frame()?;
                let (_, performative, _) = unwrap_frame(frame)?;

                if let Some(performative) = performative {
                    match performative {
                        Performative::Close(close) => {
                            event_buffer.push(Event::RemoteClose(self.id, close));
                            self.state = ConnectionState::End;
                        }
                        _ => {
                            trace!("Ignoring other frames than close");
                        }
                    }
                }
            }
            ConnectionState::End => {}
        }
        self.flush()
    }

    fn update_connection_info(self: &mut Self, open: &Open) {
        self.remote_container_id = open.container_id.clone();
        self.remote_idle_timeout = Duration::from_millis(open.idle_timeout.unwrap_or(0) as u64);
        self.remote_channel_max = open.channel_max.unwrap_or(65535);
    }

    // Write outgoing frames
    fn flush(self: &mut Self) -> Result<()> {
        for frame in self.tx_frames.drain(..) {
            trace!("TX {:?}", frame);
            self.transport.write_frame(&frame)?;
        }
        Ok(())
    }

    // Process work to be performed on sub endpoints
    fn process_work(self: &mut Self, event_buffer: &mut EventBuffer) -> Result<()> {
        for (_channel_id, session) in self.sessions.iter_mut() {
            match session.state {
                SessionState::Unmapped => {
                    if session.opened {
                        Self::local_begin(session, self.id, &mut self.tx_frames, event_buffer);
                        session.state = SessionState::BeginSent;
                    }
                }
                SessionState::BeginRcvd => {
                    if session.opened {
                        Self::local_begin(session, self.id, &mut self.tx_frames, event_buffer);
                        session.state = SessionState::Mapped;
                    }
                }
                SessionState::BeginSent | SessionState::Mapped => {
                    // Check for local sessions opened
                    for (_name, link) in session.links.iter_mut() {
                        match link.state {
                            LinkState::Unmapped => {
                                if link.opened {
                                    trace!(
                                        "Session local_channel: {:?}. Remote channel: {:?}",
                                        session.local_channel,
                                        session.remote_channel,
                                    );
                                    Self::local_attach(
                                        link,
                                        session.local_channel,
                                        self.id,
                                        &mut self.tx_frames,
                                        event_buffer,
                                    );
                                    link.state = LinkState::AttachSent;
                                }
                            }
                            LinkState::AttachSent | LinkState::Mapped => {
                                let handle = link.handle;
                                if link.role == LinkRole::Sender {
                                    for delivery in link.tx.drain(..) {
                                        let delivery_id = session.next_outgoing_id;
                                        session.next_outgoing_id += 1;
                                        Self::transfer(
                                            handle,
                                            delivery_id,
                                            session.local_channel,
                                            &mut self.tx_frames,
                                            delivery,
                                        )?;
                                    }
                                } else {
                                    for amount in link.flow.drain(..) {
                                        Self::flow(
                                            handle,
                                            session.next_outgoing_id,
                                            session.local_channel,
                                            &mut self.tx_frames,
                                            amount,
                                        )?;
                                    }
                                }
                            }
                            _ => return Err(AmqpError::not_implemented()),
                        }
                    }
                }
                _ => return Err(AmqpError::not_implemented()),
            }
        }
        Ok(())
    }

    fn keepalive(self: &mut Self, event_buffer: &mut EventBuffer) {
        // Sent out keepalives...
        let now = Instant::now();
        if self.remote_idle_timeout.as_millis() > 0 {
            /*
            trace!(
                "Remote idle timeout millis: {:?}. Last sent: {:?}",
                self.remote_idle_timeout.as_millis(),
                now - self.transport.last_sent()
            );
            */
            if now - self.transport.last_sent() >= self.remote_idle_timeout {
                let frame = Frame::AMQP(AmqpFrame {
                    channel: 0,
                    performative: None,
                    payload: None,
                });
                self.tx_frames.push(frame);
            }
        }

        if self.idle_timeout.as_millis() > 0 {
            // Ensure our peer honors our keepalive
            if now - self.transport.last_received() > self.idle_timeout * 2 {
                self.close_condition = Some(ErrorCondition {
                    condition: condition::RESOURCE_LIMIT_EXCEEDED.to_string(),
                    description: "local-idle-timeout expired".to_string(),
                });
                self.local_close(event_buffer);
            }
        }
    }

    // Dispatch frame to relevant endpoint
    fn process_frame(self: &mut Self, frame: Frame, event_buffer: &mut EventBuffer) -> Result<()> {
        let (channel_id, performative, payload) = unwrap_frame(frame)?;

        if performative.is_none() {
            return Ok(());
        }

        let performative = performative.unwrap();
        self.process_frame_internal(channel_id, &performative, payload, event_buffer)
    }

    // Handle frames for a connection
    fn process_frame_internal(
        self: &mut Self,
        channel_id: ChannelId,
        performative: &Performative,
        payload: Option<Vec<u8>>,
        event_buffer: &mut EventBuffer,
    ) -> Result<()> {
        match performative {
            // TODO: Handle sessions, links etc...
            Performative::Begin(begin) => {
                let id = self.id;

                // Response to locally initiated, use direct lookup
                let session = if let Some(remote_channel) = begin.remote_channel {
                    self.remote_channel_map.insert(channel_id, remote_channel);
                    self.sessions.get_mut(&remote_channel).unwrap()
                } else {
                    // Create session with desired settings
                    self.session_internal(Some(channel_id))
                };

                if let Some(handle_max) = begin.handle_max {
                    session.handle_max = handle_max;
                }

                session.next_outgoing_id = begin.next_outgoing_id;

                session.remote_channel = Some(channel_id);

                // let local_channel = session.local_channel;
                event_buffer.push(Event::RemoteBegin(id, session.local_channel, begin.clone()));
                match session.state {
                    SessionState::BeginSent => {
                        session.state = SessionState::Mapped;
                        Ok(())
                    }
                    SessionState::Unmapped => {
                        session.state = SessionState::BeginRcvd;
                        Ok(())
                    }
                    _ => Err(AmqpError::framing_error()),
                }
            }
            Performative::Attach(attach) => {
                trace!(
                    "Remote ATTACH to channel {:?}. Map: {:?}",
                    channel_id,
                    self.remote_channel_map
                );
                let local_channel_opt = self.remote_channel_map.get_mut(&channel_id);
                if let Some(local_channel) = local_channel_opt {
                    let session = self.sessions.get_mut(&local_channel).unwrap();
                    match session.links.get_mut(&attach.handle) {
                        None => Err(AmqpError::internal_error()),
                        Some(link) => match link.state {
                            LinkState::Unmapped => Err(AmqpError::framing_error()),
                            LinkState::AttachSent => {
                                link.state = LinkState::Mapped;
                                event_buffer.push(Event::RemoteAttach(
                                    self.id,
                                    session.local_channel,
                                    link.handle,
                                    attach.clone(),
                                ));
                                return Ok(());
                            }

                            _ => Err(AmqpError::framing_error()),
                        },
                    }
                } else {
                    Err(AmqpError::framing_error())
                }
            }
            Performative::Flow(flow) => {
                let local_channel_opt = self.remote_channel_map.get_mut(&channel_id);
                if let Some(local_channel) = local_channel_opt {
                    let session = self.sessions.get_mut(&local_channel).unwrap();
                    if let Some(handle) = flow.handle {
                        event_buffer.push(Event::Flow(
                            self.id,
                            session.local_channel,
                            handle,
                            flow.clone(),
                        ));
                    }
                }
                Ok(())
            }
            Performative::Transfer(transfer) => {
                let mut input = payload.unwrap();
                let message = Message::decode(&mut input)?;
                let delivery = Delivery {
                    state: transfer.state.clone(),
                    tag: transfer.delivery_tag.clone().unwrap(),
                    remotely_settled: transfer.settled.unwrap_or(false),
                    settled: false,
                    message: message,
                };
                event_buffer.push(Event::Delivery(
                    self.id,
                    channel_id,
                    transfer.handle,
                    delivery,
                ));
                Ok(())
            }
            Performative::Disposition(disposition) => {
                let local_channel_opt = self.remote_channel_map.get_mut(&channel_id);
                if let Some(local_channel) = local_channel_opt {
                    let session = self.sessions.get_mut(&local_channel).unwrap();
                    event_buffer.push(Event::Disposition(
                        self.id,
                        session.local_channel,
                        disposition.clone(),
                    ));
                }
                Ok(())
            }
            Performative::Detach(detach) => {
                let local_channel_opt = self.remote_channel_map.get_mut(&channel_id);
                // Lookup session and remove link
                if let Some(local_channel) = local_channel_opt {
                    let session = self.sessions.get_mut(&local_channel).unwrap();
                    session.links.remove(&detach.handle);
                }

                if let Some(error) = detach.error.clone() {
                    Err(AmqpError::Amqp(error))
                } else {
                    Ok(())
                }
            }
            Performative::End(end) => {
                let local_channel_opt = self.remote_channel_map.get_mut(&channel_id);
                if let Some(local_channel) = local_channel_opt {
                    self.sessions.remove(&local_channel);
                    self.remote_channel_map.remove(&channel_id);
                }
                if let Some(error) = end.error.clone() {
                    Err(AmqpError::Amqp(error))
                } else {
                    Ok(())
                }
            }
            Performative::Close(close) => {
                if channel_id == 0 {
                    let id = self.id;
                    event_buffer.push(Event::RemoteClose(id, close.clone()));
                    self.sessions.clear();
                    self.state = ConnectionState::CloseRcvd;
                }
                Ok(())
            }
            _ => Err(AmqpError::framing_error()),
        }
    }

    fn local_open(self: &mut Self, event_buffer: &mut EventBuffer) {
        let mut args = Open::new(self.container_id.as_str());
        args.hostname = Some(self.hostname.clone());
        args.channel_max = Some(self.channel_max);
        args.idle_timeout = Some(self.idle_timeout.as_millis() as u32);

        let frame = Frame::AMQP(AmqpFrame {
            channel: 0,
            performative: Some(Performative::Open(args.clone())),
            payload: None,
        });

        self.tx_frames.push(frame);
        event_buffer.push(Event::LocalOpen(self.id, args));
    }

    fn local_close(self: &mut Self, event_buffer: &mut EventBuffer) {
        let frame = Frame::AMQP(AmqpFrame {
            channel: 0,
            performative: Some(Performative::Close(Close {
                error: self.close_condition.clone(),
            })),
            payload: None,
        });

        self.tx_frames.push(frame);

        let condition = self.close_condition.clone();
        event_buffer.push(Event::LocalClose(self.id, condition));
    }

    fn local_begin(
        session: &Session,
        connection_id: ConnectionId,
        tx_frames: &mut Vec<Frame>,
        event_buffer: &mut EventBuffer,
    ) {
        let data = Begin {
            remote_channel: session.remote_channel,
            next_outgoing_id: 0,
            incoming_window: 10,
            outgoing_window: 10,
            handle_max: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };
        let frame = Frame::AMQP(AmqpFrame {
            channel: session.local_channel as u16,
            performative: Some(Performative::Begin(data.clone())),
            payload: None,
        });

        tx_frames.push(frame);
        event_buffer.push(Event::LocalBegin(
            connection_id,
            session.local_channel,
            data,
        ));
    }

    fn local_attach(
        link: &Link,
        local_channel: ChannelId,
        connection_id: ConnectionId,
        tx_frames: &mut Vec<Frame>,
        event_buffer: &mut EventBuffer,
    ) {
        let data = Attach {
            name: link.name.clone(),
            handle: link.handle as u32,
            role: link.role,
            snd_settle_mode: None,
            rcv_settle_mode: None,
            source: link.source.clone(),
            target: link.target.clone(),
            unsettled: None,
            incomplete_unsettled: None,
            initial_delivery_count: if link.role == LinkRole::Sender {
                Some(0)
            } else {
                None
            },
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };

        let frame = Frame::AMQP(AmqpFrame {
            channel: local_channel,
            performative: Some(Performative::Attach(data.clone())),
            payload: None,
        });

        tx_frames.push(frame);
        event_buffer.push(Event::LocalAttach(
            connection_id,
            local_channel,
            link.handle,
            data,
        ));
    }

    fn transfer(
        handle: HandleId,
        delivery_id: u32,
        local_channel: ChannelId,
        tx_frames: &mut Vec<Frame>,
        delivery: Delivery,
    ) -> Result<()> {
        let t = Transfer {
            handle: handle,
            delivery_id: Some(delivery_id),
            delivery_tag: Some(delivery.tag),
            message_format: Some(0),
            settled: Some(false),
            more: Some(false),
            rcv_settle_mode: None,
            state: None,
            resume: None,
            aborted: None,
            batchable: None,
        };

        trace!("TX MESSAGE: {:?}", delivery.message);
        let mut msgbuf = Vec::new();
        delivery.message.encode(&mut msgbuf)?;

        let frame = Frame::AMQP(AmqpFrame {
            channel: local_channel,
            performative: Some(Performative::Transfer(t)),
            payload: Some(msgbuf),
        });

        tx_frames.push(frame);
        Ok(())
    }

    fn flow(
        handle: HandleId,
        next_outgoing_id: u32,
        local_channel: ChannelId,
        tx_frames: &mut Vec<Frame>,
        amount: u32,
    ) -> Result<()> {
        let f = Flow {
            next_incoming_id: None,
            incoming_window: std::i32::MAX as u32,
            next_outgoing_id: next_outgoing_id,
            outgoing_window: std::i32::MAX as u32,
            handle: Some(handle as u32),
            delivery_count: None,
            link_credit: Some(amount),
            available: None,
            drain: None,
            echo: None,
            properties: None,
        };

        let frame = Frame::AMQP(AmqpFrame {
            channel: local_channel,
            performative: Some(Performative::Flow(f)),
            payload: None,
        });

        tx_frames.push(frame);
        Ok(())
    }
}

impl Session {
    pub fn open(self: &mut Self) {
        self.opened = true;
    }

    pub fn get_link(self: &mut Self, handle_id: HandleId) -> Option<&mut Link> {
        self.links.get_mut(&handle_id)
    }

    fn allocate_handle(self: &mut Self) -> Option<HandleId> {
        for i in 0..self.handle_max {
            let id = i as HandleId;
            if !self.links.contains_key(&id) {
                return Some(id);
            }
        }
        None
    }

    pub fn create_sender<'a>(self: &mut Self, address: Option<&'a str>) -> &mut Link {
        let name = address.unwrap_or("unknown").to_string();
        let id = self.allocate_handle().unwrap();
        self.links.insert(
            id,
            Link {
                name: name,
                handle: id,
                next_message_id: 0,
                role: LinkRole::Sender,
                state: LinkState::Unmapped,
                source: Some(Source {
                    address: None,
                    durable: None,
                    expiry_policy: None,
                    timeout: None,
                    dynamic: None,
                    dynamic_node_properties: None,
                    default_outcome: None,
                    distribution_mode: None,
                    filter: None,
                    outcomes: None,
                    capabilities: None,
                }),
                target: Some(Target {
                    address: address.map(|s| s.to_string()),
                    durable: None,
                    expiry_policy: None,
                    timeout: None,
                    dynamic: Some(false),
                    dynamic_node_properties: None,
                    capabilities: None,
                }),
                opened: false,
                tx: Vec::new(),
                flow: Vec::new(),
            },
        );
        self.links.get_mut(&id).unwrap()
    }

    pub fn create_receiver<'a>(self: &mut Self, address: Option<&'a str>) -> &mut Link {
        let name = address.unwrap_or("unknown").to_string();
        let id = self.allocate_handle().unwrap();
        self.links.insert(
            id,
            Link {
                name: name,
                handle: id,
                next_message_id: 0,
                role: LinkRole::Receiver,
                state: LinkState::Unmapped,
                source: Some(Source {
                    address: address.map(|s| s.to_string()),
                    durable: None,
                    expiry_policy: None,
                    timeout: None,
                    dynamic: Some(false),
                    dynamic_node_properties: None,
                    default_outcome: None,
                    distribution_mode: None,
                    filter: None,
                    outcomes: None,
                    capabilities: None,
                }),
                target: Some(Target {
                    address: address.map(|s| s.to_string()),
                    durable: None,
                    expiry_policy: None,
                    timeout: None,
                    dynamic: Some(false),
                    dynamic_node_properties: None,
                    capabilities: None,
                }),
                opened: false,
                tx: Vec::new(),
                flow: Vec::new(),
            },
        );
        self.links.get_mut(&id).unwrap()
    }
}

impl Link {
    pub fn open(self: &mut Self) {
        self.opened = true;
    }

    pub fn flow(self: &mut Self, credits: u32) {
        self.flow.push(credits);
    }

    pub fn send(self: &mut Self, data: &str) {
        let mut message = Message::amqp_value(Value::String(data.to_string()));
        message.properties = Some(MessageProperties {
            message_id: Some(Value::Ulong(self.next_message_id + 1)),
            user_id: None,
            to: None,
            subject: None,
            reply_to: None,
            correlation_id: None,
            content_type: None,
            content_encoding: None,
            absolute_expiry_time: None,
            creation_time: None,
            group_id: None,
            group_sequence: None,
            reply_to_group_id: None,
        });
        self.next_message_id += 1;

        let delivery_tag = rand::thread_rng().gen::<[u8; 16]>();

        self.tx.push(Delivery {
            message: message,
            tag: delivery_tag.to_vec(),
            state: None,
            remotely_settled: false,
            settled: false,
        });
    }
}
