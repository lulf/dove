/*
 * Copyright 2019-2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The driver module contains a event-based connection driver that drives the connection state machine of one or more connections.

use log::trace;
use mio::event;
use mio::{Interest, Registry, Token};
use rand::Rng;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;
use std::vec::Vec;

use crate::conn::*;
use crate::error::*;
use crate::framing::*;
use crate::message::*;
use crate::types::*;

#[derive(Debug)]
pub struct ConnectionDriver {
    pub id: ConnectionId,
    pub container_id: String,
    pub hostname: String,
    pub channel_max: u16,
    pub idle_timeout: Duration,
    pub remote_idle_timeout: Duration,
    pub remote_container_id: String,
    pub remote_channel_max: u16,
    connection: Connection,
    state: ConnectionState,
    opened: bool,
    closed: bool,
    close_condition: Option<ErrorCondition>,
    sessions: HashMap<ChannelId, Session>,
    remote_channel_map: HashMap<ChannelId, ChannelId>,
}

#[derive(Debug, PartialEq, Eq)]
enum ConnectionState {
    Start,
    Opening,
    Opened,
    Closing,
    CloseSent,
    Closed,
}

type ConnectionId = usize;
type HandleId = u32;

#[allow(dead_code)]
#[derive(Debug)]
enum SessionState {
    Opening,
    Opened,
    Closing,
    Closed,
}

#[allow(dead_code)]
#[derive(Debug)]
enum LinkState {
    Opening,
    Opened,
    Closing,
    Closed,
}

#[derive(Debug)]
pub struct Session {
    pub local_channel: ChannelId,
    end_condition: Option<ErrorCondition>,
    remote_channel: Option<ChannelId>,
    handle_max: u32,
    state: SessionState,
    opened: bool,
    closed: bool,
    links: HashMap<HandleId, Link>,
    next_outgoing_id: u32,
    unacked: HashSet<u32>,
}

#[derive(Debug)]
pub struct Link {
    name: String,
    handle: HandleId,
    opened: bool,
    closed: bool,
    pub role: LinkRole,
    source: Option<Source>,
    target: Option<Target>,
    state: LinkState,
    next_message_id: u64,
    deliveries: Vec<Delivery>,
    dispositions: Vec<DeliveryDisposition>,
    flow: Vec<u32>,
}

#[derive(Debug)]
pub struct Delivery {
    pub message: Message,
    remotely_settled: bool,
    settled: bool,
    state: Option<DeliveryState>,
    tag: Vec<u8>,
    id: u32,
}

#[derive(Debug)]
pub struct DeliveryDisposition {
    id: u32,
    settled: bool,
    state: DeliveryState,
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
    LocalEnd(ConnectionId, ChannelId, End),
    RemoteBegin(ConnectionId, ChannelId, Begin),
    RemoteEnd(ConnectionId, ChannelId, End),

    LocalAttach(ConnectionId, ChannelId, HandleId, Attach),
    RemoteAttach(ConnectionId, ChannelId, HandleId, Attach),

    LocalDetach(ConnectionId, ChannelId, HandleId, Detach),
    RemoteDetach(ConnectionId, ChannelId, HandleId, Detach),

    Flow(ConnectionId, ChannelId, HandleId, Flow),
    Disposition(ConnectionId, ChannelId, Disposition),
    Delivery(ConnectionId, ChannelId, HandleId, Delivery),
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

impl ConnectionDriver {
    pub fn new(id: ConnectionId, connection: Connection) -> ConnectionDriver {
        let hostname = connection.hostname.clone();
        ConnectionDriver {
            id: id,
            container_id: "dove".to_string(),
            hostname: hostname,
            connection: connection,
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
            end_condition: None,
            remote_channel: channel_id,
            local_channel: chan,
            handle_max: std::u32::MAX,
            opened: false,
            closed: false,
            next_outgoing_id: 0,
            state: SessionState::Opening,
            links: HashMap::new(),
            unacked: HashSet::new(),
        };
        self.sessions.insert(chan, s);
        channel_id.map(|c| self.remote_channel_map.insert(c, chan));
        self.sessions.get_mut(&chan).unwrap()
    }

    pub fn close(self: &mut Self, condition: Option<ErrorCondition>) {
        self.closed = true;
        self.close_condition = condition;
    }

    pub fn token(self: &Self) -> Token {
        Token(self.id)
    }

    /**
     * Do work on this connection until progress cannot be made.
     */
    pub fn do_work(self: &mut Self, event_buffer: &mut EventBuffer) -> Result<()> {
        let output_result = self.process_output(event_buffer);
        let input_result = self.process_input(event_buffer);
        match (output_result, input_result) {
            (Err(e), _) => Err(e),
            (_, Err(e)) => Err(e),
            _ => Ok(()),
        }
    }

    fn process_output(self: &mut Self, event_buffer: &mut EventBuffer) -> Result<()> {
        match self.state {
            ConnectionState::Start => {
                event_buffer.push(Event::ConnectionInit(self.id));
                self.state = ConnectionState::Opening;
            }
            ConnectionState::Opening => {
                if self.opened {
                    let mut open = Open::new(self.container_id.as_str());
                    open.hostname = Some(self.hostname.clone());
                    open.channel_max = Some(self.channel_max);
                    open.idle_timeout = Some(self.idle_timeout.as_millis() as u32);

                    self.connection.open(open.clone())?;
                    event_buffer.push(Event::LocalOpen(self.id, open));
                    self.state = ConnectionState::Opened;
                }
            }
            ConnectionState::Opened => {
                if self.closed {
                    self.state = ConnectionState::Closing;
                } else {
                    self.process_connection(event_buffer)?;
                    self.keepalive()?;
                }
            }
            ConnectionState::Closing => {
                if self.closed {
                    self.connection.close(Close {
                        error: self.close_condition.clone(),
                    })?;

                    let condition = self.close_condition.clone();
                    event_buffer.push(Event::LocalClose(self.id, condition));
                }
                self.state = ConnectionState::CloseSent;
            }
            ConnectionState::CloseSent => {}
            _ => return Ok(()),
        }
        Ok(())
    }

    fn process_input(self: &mut Self, event_buffer: &mut EventBuffer) -> Result<()> {
        // Process connection with new input
        let mut rx_frames = Vec::new();
        self.connection.process(&mut rx_frames)?;
        for frame in rx_frames.drain(..) {
            self.process_frame(frame, event_buffer)?;
        }
        Ok(())
    }

    // Process work to be performed on sub endpoints
    fn process_connection(self: &mut Self, event_buffer: &mut EventBuffer) -> Result<()> {
        for (_, session) in self.sessions.iter_mut() {
            session.process(self.id, &mut self.connection, event_buffer)?;
        }
        Ok(())
    }

    fn keepalive(self: &mut Self) -> Result<()> {
        // Sent out keepalives...
        let now = Instant::now();

        let last_received = self.connection.keepalive(self.remote_idle_timeout, now)?;
        if self.idle_timeout.as_millis() > 0 {
            // Ensure our peer honors our keepalive
            if now - last_received > self.idle_timeout * 2 {
                self.close_condition = Some(ErrorCondition {
                    condition: condition::RESOURCE_LIMIT_EXCEEDED.to_string(),
                    description: "local-idle-timeout expired".to_string(),
                });
                self.closed = true;
            }
        }
        Ok(())
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
        trace!("State: {:?}. Performative: {:?}", self.state, performative);
        match performative {
            Performative::Open(open) => {
                if self.state != ConnectionState::Opening && self.state != ConnectionState::Opened {
                    return Err(AmqpError::framing_error());
                }
                self.remote_container_id = open.container_id.clone();
                self.remote_idle_timeout =
                    Duration::from_millis(open.idle_timeout.unwrap_or(0) as u64);
                self.remote_channel_max = open.channel_max.unwrap_or(65535);
                event_buffer.push(Event::RemoteOpen(self.id, open.clone()));
                Ok(())
            }
            Performative::Begin(begin) => {
                if self.state == ConnectionState::Opening || self.state == ConnectionState::Closed {
                    return Err(AmqpError::framing_error());
                }
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
                    SessionState::Closed => Err(AmqpError::framing_error()),
                    _ => Ok(()),
                }
            }
            Performative::Attach(attach) => {
                if self.state == ConnectionState::Opening || self.state == ConnectionState::Closed {
                    return Err(AmqpError::framing_error());
                }
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
                            LinkState::Closed => Err(AmqpError::framing_error()),
                            _ => {
                                event_buffer.push(Event::RemoteAttach(
                                    self.id,
                                    session.local_channel,
                                    link.handle,
                                    attach.clone(),
                                ));
                                return Ok(());
                            }
                        },
                    }
                } else {
                    Err(AmqpError::framing_error())
                }
            }
            Performative::Flow(flow) => {
                if self.state == ConnectionState::Opening || self.state == ConnectionState::Closed {
                    return Err(AmqpError::framing_error());
                }
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
                if self.state == ConnectionState::Opening || self.state == ConnectionState::Closed {
                    return Err(AmqpError::framing_error());
                }
                let mut input = payload.unwrap();
                let message = Message::decode(&mut input)?;
                let delivery = Delivery {
                    state: transfer.state.clone(),
                    tag: transfer.delivery_tag.clone().unwrap(),
                    id: transfer.delivery_id.unwrap(),
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
                if self.state == ConnectionState::Opening || self.state == ConnectionState::Closed {
                    return Err(AmqpError::framing_error());
                }
                let local_channel_opt = self.remote_channel_map.get_mut(&channel_id);
                if let Some(local_channel) = local_channel_opt {
                    let session = self.sessions.get_mut(&local_channel).unwrap();
                    let last = disposition.last.unwrap_or(disposition.first);
                    for id in disposition.first..=last {
                        session.unacked.remove(&id);
                    }
                    event_buffer.push(Event::Disposition(
                        self.id,
                        session.local_channel,
                        disposition.clone(),
                    ));
                }
                Ok(())
            }
            Performative::Detach(detach) => {
                if self.state == ConnectionState::Opening || self.state == ConnectionState::Closed {
                    return Err(AmqpError::framing_error());
                }
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
                if self.state == ConnectionState::Opening || self.state == ConnectionState::Closed {
                    return Err(AmqpError::framing_error());
                }
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
                if self.state == ConnectionState::Closed {
                    return Err(AmqpError::framing_error());
                }
                if channel_id == 0 {
                    let id = self.id;
                    event_buffer.push(Event::RemoteClose(id, close.clone()));
                    self.sessions.clear();
                    if self.state == ConnectionState::CloseSent {
                        self.state = ConnectionState::Closed;
                    } else if self.state != ConnectionState::Closed {
                        self.state = ConnectionState::Closing;
                    }
                }
                Ok(())
            }
        }
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
                state: LinkState::Opening,
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
                closed: false,
                deliveries: Vec::new(),
                dispositions: Vec::new(),
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
                state: LinkState::Opening,
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
                closed: false,
                deliveries: Vec::new(),
                dispositions: Vec::new(),
                flow: Vec::new(),
            },
        );
        self.links.get_mut(&id).unwrap()
    }

    fn process(
        self: &mut Self,
        connection_id: ConnectionId,
        connection: &mut Connection,
        event_buffer: &mut EventBuffer,
    ) -> Result<()> {
        match self.state {
            SessionState::Opening => {
                if self.opened {
                    let begin = Begin {
                        remote_channel: self.remote_channel,
                        next_outgoing_id: 0,
                        incoming_window: 10,
                        outgoing_window: 10,
                        handle_max: None,
                        offered_capabilities: None,
                        desired_capabilities: None,
                        properties: None,
                    };
                    connection.begin(self.local_channel, begin.clone())?;
                    event_buffer.push(Event::LocalBegin(connection_id, self.local_channel, begin));
                    self.state = SessionState::Opened;
                }
                Ok(())
            }
            SessionState::Opened => {
                if self.closed {
                    let end = End {
                        error: self.end_condition.clone(),
                    };
                    connection.end(self.local_channel, end.clone())?;
                    event_buffer.push(Event::LocalEnd(connection_id, self.local_channel, end));
                    self.state = SessionState::Closing;
                } else {
                    for (_, link) in self.links.iter_mut() {
                        self.next_outgoing_id = link.process(
                            connection_id,
                            connection,
                            &mut self.unacked,
                            self.local_channel,
                            self.next_outgoing_id,
                            event_buffer,
                        )?;
                    }
                }
                Ok(())
            }
            SessionState::Closing | SessionState::Closed => Err(AmqpError::not_implemented()),
        }
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

        let delivery = Delivery {
            message: message,
            id: 0, // Set by transfer
            tag: delivery_tag.to_vec(),
            state: None,
            remotely_settled: false,
            settled: false,
        };

        self.deliveries.push(delivery);
    }

    pub fn settle(self: &mut Self, delivery: &Delivery, settled: bool, state: DeliveryState) {
        self.dispositions.push(DeliveryDisposition {
            id: delivery.id,
            settled: settled,
            state: state,
        });
    }

    fn process(
        self: &mut Self,
        connection_id: ConnectionId,
        connection: &mut Connection,
        unacked: &mut HashSet<u32>,
        local_channel: ChannelId,
        next_outgoing_id: u32,
        event_buffer: &mut EventBuffer,
    ) -> Result<u32> {
        match self.state {
            LinkState::Opening => {
                if self.opened {
                    let attach = Attach {
                        name: self.name.clone(),
                        handle: self.handle as u32,
                        role: self.role,
                        snd_settle_mode: None,
                        rcv_settle_mode: None,
                        source: self.source.clone(),
                        target: self.target.clone(),
                        unsettled: None,
                        incomplete_unsettled: None,
                        initial_delivery_count: if self.role == LinkRole::Sender {
                            Some(0)
                        } else {
                            None
                        },
                        max_message_size: None,
                        offered_capabilities: None,
                        desired_capabilities: None,
                        properties: None,
                    };
                    connection.attach(local_channel, attach.clone())?;

                    event_buffer.push(Event::LocalAttach(
                        connection_id,
                        local_channel,
                        self.handle,
                        attach,
                    ));
                    self.state = LinkState::Opened;
                }
                Ok(next_outgoing_id)
            }
            LinkState::Opened => {
                if self.closed {
                    Err(AmqpError::not_implemented())
                } else {
                    if self.role == LinkRole::Sender {
                        let mut next_id = next_outgoing_id;
                        for delivery in self.deliveries.drain(..) {
                            let delivery_id = next_id;
                            next_id += 1;

                            trace!("TX MESSAGE: {:?}", delivery.message);
                            let mut msgbuf = Vec::new();
                            delivery.message.encode(&mut msgbuf)?;

                            unacked.insert(delivery_id);
                            connection.transfer(
                                local_channel,
                                Transfer {
                                    handle: self.handle,
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
                                },
                                Some(msgbuf),
                            )?;
                        }
                        Ok(next_id)
                    } else {
                        for disposition in self.dispositions.drain(..) {
                            connection.disposition(
                                local_channel,
                                Disposition {
                                    role: self.role,
                                    first: disposition.id,
                                    last: None,
                                    settled: Some(disposition.settled),
                                    state: Some(disposition.state),
                                    batchable: None,
                                },
                            )?;
                        }
                        for amount in self.flow.drain(..) {
                            connection.flow(
                                local_channel,
                                Flow {
                                    next_incoming_id: None,
                                    incoming_window: std::i32::MAX as u32,
                                    next_outgoing_id: next_outgoing_id,
                                    outgoing_window: std::i32::MAX as u32,
                                    handle: Some(self.handle as u32),
                                    delivery_count: None,
                                    link_credit: Some(amount),
                                    available: None,
                                    drain: None,
                                    echo: None,
                                    properties: None,
                                },
                            )?;
                        }
                        Ok(next_outgoing_id)
                    }
                }
            }
            LinkState::Closing | LinkState::Closed => Err(AmqpError::not_implemented()),
        }
    }
}

impl event::Source for ConnectionDriver {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> std::io::Result<()> {
        self.connection.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> std::io::Result<()> {
        self.connection.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> std::io::Result<()> {
        self.connection.deregister(registry)
    }
}
