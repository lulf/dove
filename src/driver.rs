/*
 * Copyright 2019-2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The driver module is an intermediate layer with the core logic for interacting with different AMQP 1.0 endpoint entities (connections, sessions, links).

use crate::conn::ChannelId;
use crate::connection::ConnectionHandle;
use crate::error::*;
use crate::framing;
use crate::framing::{
    AmqpFrame, Attach, Begin, Close, DeliveryState, Detach, End, Flow, Frame, LinkRole, Open,
    Performative, Source, Target, Transfer,
};
use crate::message::Message;
use crate::options::LinkOptions;
use rand::Rng;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

pub type DeliveryTag = Vec<u8>;
pub type HandleId = u32;

#[derive(Debug)]
pub struct ConnectionDriver {
    channel_max: u16,
    idle_timeout: Duration,
    connection: ConnectionHandle,
    sessions: Mutex<HashMap<ChannelId, Arc<SessionDriver>>>,

    // Frames received on this connection
    rx: Channel<AmqpFrame>,
    remote_channel_map: Mutex<HashMap<ChannelId, ChannelId>>,
    remote_idle_timeout: Duration,

    // State
    closed: AtomicBool,
}

#[derive(Debug)]
pub struct SessionDriver {
    // Frames received on this session
    connection: ConnectionHandle,
    local_channel: ChannelId,
    rx: Channel<AmqpFrame>,

    links_in_flight: Mutex<HashMap<String, Arc<LinkDriver>>>,
    links: Mutex<HashMap<HandleId, Arc<LinkDriver>>>,
    handle_generator: AtomicU32,

    #[allow(clippy::type_complexity)]
    did_to_delivery: Arc<Mutex<HashMap<u32, (HandleId, Arc<DeliveryDriver>)>>>,
    initial_outgoing_id: u32,

    flow_control: Arc<Mutex<SessionFlowControl>>,
    link_mapping: Arc<Mutex<HashMap<u32, u32>>>,
}

// TODO: Make this use atomic operations
#[derive(Clone, Debug)]
struct SessionFlowControl {
    next_outgoing_id: u32,
    next_incoming_id: u32,

    incoming_window: u32,
    outgoing_window: u32,

    remote_incoming_window: u32,
    remote_outgoing_window: u32,
}

impl SessionFlowControl {
    fn new() -> SessionFlowControl {
        SessionFlowControl {
            next_outgoing_id: 0,
            next_incoming_id: 0,

            incoming_window: std::i32::MAX as u32,
            outgoing_window: std::i32::MAX as u32,

            remote_incoming_window: 0,
            remote_outgoing_window: 0,
        }
    }

    fn accept(&mut self, delivery_id: u32) -> Result<bool> {
        if delivery_id + 1 < self.next_incoming_id || self.remote_outgoing_window == 0 {
            Err(AmqpError::framing_error(None))
        } else if self.incoming_window == 0 {
            Ok(false)
        } else {
            self.incoming_window -= 1;
            self.next_incoming_id = delivery_id + 1;
            self.remote_outgoing_window -= 1;
            Ok(true)
        }
    }

    fn next(&mut self) -> Option<SessionFlowControl> {
        if self.outgoing_window > 0 && self.remote_incoming_window > 0 {
            let original = self.clone();
            self.next_outgoing_id += 1;
            self.outgoing_window -= 1;
            self.remote_incoming_window -= 1;
            Some(original)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct LinkDriver {
    pub name: String,
    pub handle: u32,
    pub role: LinkRole,
    pub channel: ChannelId,
    connection: ConnectionHandle,
    rx: Channel<AmqpFrame>,

    session_flow_control: Arc<Mutex<SessionFlowControl>>,

    #[allow(clippy::type_complexity)]
    did_to_delivery: Arc<Mutex<HashMap<u32, (HandleId, Arc<DeliveryDriver>)>>>,
    credit: AtomicU32,
    delivery_count: AtomicU32,
}

#[derive(Debug)]
pub struct DeliveryDriver {
    pub message: Option<Message>,
    pub remotely_settled: bool,
    pub settled: bool,
    pub state: Option<DeliveryState>,
    pub tag: DeliveryTag,
    pub id: u32,
}

pub struct SessionOpts {
    pub max_frame_size: u32,
}

impl ConnectionDriver {
    pub fn new(connection: ConnectionHandle, idle_timeout: Duration) -> ConnectionDriver {
        ConnectionDriver {
            connection,
            rx: Channel::new(),
            sessions: Mutex::new(HashMap::new()),
            remote_channel_map: Mutex::new(HashMap::new()),
            idle_timeout,
            remote_idle_timeout: Duration::from_secs(0),
            channel_max: u16::MAX,
            closed: AtomicBool::new(false),
        }
    }

    pub fn closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub fn connection(&self) -> &ConnectionHandle {
        &self.connection
    }

    pub fn flowcontrol(&self) -> Result<()> {
        let low_flow_watermark = 100;
        let high_flow_watermark = 1000;

        for (_, session) in self.sessions.lock().unwrap().iter_mut() {
            for (_, link) in session.links.lock().unwrap().iter_mut() {
                if link.role == LinkRole::Receiver {
                    let credit = link.credit.load(Ordering::SeqCst);
                    if credit <= low_flow_watermark {
                        link.flow(high_flow_watermark)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn keepalive(&self) -> Result<()> {
        let now = Instant::now();
        let last_received = self.connection.keepalive(self.remote_idle_timeout, now)?;

        if self.idle_timeout.as_millis() > 0 {
            // Ensure our peer honors our keepalive
            if now - last_received > self.idle_timeout * 2 {
                self.connection.close(Close {
                    error: Some(ErrorCondition::local_idle_timeout()),
                })?;
                warn!("Connection timed out");
                return Err(AmqpError::IoError(std::io::Error::from(
                    std::io::ErrorKind::TimedOut,
                )));
            }
        }
        Ok(())
    }

    pub fn open(&self, open: Open) -> Result<()> {
        self.connection.open(open)
    }

    pub fn close(&self, error: Option<ErrorCondition>) -> Result<()> {
        if self.closed.fetch_or(true, Ordering::SeqCst) {
            return Ok(());
        }

        for (_id, session) in core::mem::take(&mut *self.sessions.lock().unwrap()) {
            for (_id, link) in core::mem::take(&mut *session.links.lock().unwrap()) {
                let _ = link.close(None);
                link.rx.close();
            }
            let _ = session.close(None);
            session.rx.close();
        }

        self.rx.close();
        self.connection.close(Close { error })
    }

    pub(crate) fn dispatch(&self, frames: Vec<Frame>) -> Result<()> {
        for frame in frames {
            if let Frame::AMQP(frame) = frame {
                trace!("Got AMQP frame: {:?}", frame.performative);
                if let Some(ref performative) = frame.performative {
                    let channel = frame.channel;
                    match performative {
                        Performative::Open(ref _open) => {
                            self.rx.send(frame)?;
                        }
                        Performative::Close(ref _close) => {
                            self.rx.send(frame)?;
                        }
                        Performative::Begin(ref begin) => {
                            let m = self.sessions.lock().unwrap();
                            let s = m.get(&channel);
                            if let Some(s) = s {
                                {
                                    let mut f = s.flow_control.lock().unwrap();
                                    f.remote_outgoing_window = begin.outgoing_window;
                                    f.remote_incoming_window = begin.incoming_window;
                                    if let Some(remote_channel) = begin.remote_channel {
                                        let mut cm = self.remote_channel_map.lock().unwrap();
                                        cm.insert(channel, remote_channel);
                                    }
                                }
                                s.rx.send(frame)?;
                            }
                        }
                        Performative::End(ref _end) => {
                            let local_channel: Option<ChannelId> = {
                                let cm = self.remote_channel_map.lock().unwrap();
                                cm.get(&channel).cloned()
                            };

                            if let Some(local_channel) = local_channel {
                                let mut m = self.sessions.lock().unwrap();
                                m.get_mut(&local_channel).map(|s| s.rx.send(frame));
                            }
                        }
                        _ => {
                            let local_channel: Option<ChannelId> = {
                                let cm = self.remote_channel_map.lock().unwrap();
                                cm.get(&channel).cloned()
                            };

                            if let Some(local_channel) = local_channel {
                                let session = {
                                    let mut m = self.sessions.lock().unwrap();
                                    m.get_mut(&local_channel).cloned()
                                };

                                if let Some(s) = session {
                                    s.dispatch(frame)?;
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn allocate_session(&self) -> Option<Arc<SessionDriver>> {
        let mut m = self.sessions.lock().unwrap();
        for i in 0..self.channel_max {
            let chan = i as ChannelId;
            if let Entry::Vacant(entry) = m.entry(chan) {
                let session = Arc::new(SessionDriver {
                    connection: self.connection.clone(),
                    local_channel: chan,
                    rx: Channel::new(),

                    links_in_flight: Mutex::new(HashMap::new()),
                    links: Mutex::new(HashMap::new()),
                    handle_generator: AtomicU32::new(0),

                    flow_control: Arc::new(Mutex::new(SessionFlowControl::new())),
                    initial_outgoing_id: 0,

                    did_to_delivery: Arc::new(Mutex::new(HashMap::new())),
                    link_mapping: Arc::new(Mutex::new(HashMap::new())),
                });
                entry.insert(session.clone());
                return Some(session);
            }
        }
        None
    }

    pub async fn new_session(&self, _opts: Option<SessionOpts>) -> Result<Arc<SessionDriver>> {
        let session = self
            .allocate_session()
            .ok_or(AmqpError::SessionAllocationExhausted)?;
        let flow_control: SessionFlowControl = { session.flow_control.lock().unwrap().clone() };
        let begin = Begin {
            remote_channel: None,
            next_outgoing_id: flow_control.next_outgoing_id,
            incoming_window: flow_control.incoming_window,
            outgoing_window: flow_control.outgoing_window,
            handle_max: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };
        debug!(
            "Creating session with local channel {}",
            session.local_channel
        );

        self.connection.begin(session.local_channel, begin)?;
        Ok(session)
    }

    #[inline]
    pub async fn recv(&self) -> Result<AmqpFrame> {
        self.rx.recv().await
    }

    #[inline]
    pub fn unrecv(&self, frame: AmqpFrame) -> Result<()> {
        warn!("unrecv: {:?}", frame);
        self.rx.send(frame)
    }
}

impl SessionDriver {
    pub fn dispatch(&self, frame: AmqpFrame) -> Result<()> {
        trace!("Dispatching frame: {:?}", frame);
        match &frame.performative {
            Some(Performative::Attach(attach_response)) => {
                let link = self
                    .links_in_flight
                    .lock()
                    .unwrap()
                    .remove(&attach_response.name);

                if let Some(link) = link {
                    let handle = attach_response.handle;
                    self.link_mapping.lock().unwrap().insert(link.handle, handle);
                    if link.rx.send(frame).is_ok() {
                        self.links.lock().unwrap().insert(handle, Arc::clone(&link));
                    } else {
                        error!("Failed to notify LinkDriver about attach frame")
                    }
                } else {
                    error!(
                        "Received attach frame for unknown link: {:?}",
                        attach_response
                    );
                }
            }
            Some(Performative::Detach(ref detach)) => {
                if let Some(link) = self.links.lock().unwrap().remove(&detach.handle) {
                    link.rx.send(frame)?;
                } else {
                    warn!("Detach request with unknown handle received: {:?}", detach)
                }
            }
            Some(Performative::Transfer(ref transfer)) => {
                // Session flow control
                if let Some(delivery_id) = transfer.delivery_id {
                    loop {
                        let result = self.flow_control.lock().unwrap().accept(delivery_id);
                        match result {
                            Err(AmqpError::Amqp(cond)) => {
                                error!("Transfer error: {:?}", cond);
                                self.close(Some(cond))?;
                            }
                            Err(e) => {
                                error!("Transfer error: {:?}", e);
                                self.close(None)?;
                            }
                            Ok(false) => {}
                            Ok(true) => break,
                        }
                    }
                }

                let link = {
                    let mut m = self.links.lock().unwrap();
                    m.get_mut(&transfer.handle).unwrap().clone()
                };

                let count_down = |x| {
                    if x == 0 {
                        Some(0)
                    } else {
                        Some(x - 1)
                    }
                };
                // Link flow control
                if link
                    .credit
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, count_down)
                    == Ok(0)
                {
                    trace!("Transfer but no space left!");
                } else {
                    trace!(
                        "Received transfer. Credit: {:?}",
                        link.credit.load(Ordering::SeqCst)
                    );
                    link.delivery_count.fetch_add(1, Ordering::SeqCst);
                    link.rx.send(frame)?;
                }
            }
            Some(Performative::Disposition(ref disposition)) => {
                trace!("Received disposition: {:?}", disposition);
                let last = disposition.last.unwrap_or(disposition.first);
                for id in disposition.first..=last {
                    if let Some((handle, _delivery)) =
                        self.did_to_delivery.lock().unwrap().remove(&id)
                    {
                        if let Some(handle_internal) = self.link_mapping.lock().unwrap().get(&handle) { 
                            if let Some(link) = self.links.lock().unwrap().get(&handle_internal).cloned() {
                                if link.role == disposition.role {
                                    link.rx.send(frame.clone())?;
                                }
                            } else {
                                debug!("Disposition for invalid handle({}) received", handle);
                            }
                        } else {
                            debug!("Link mapping not found for {}", handle);
                        }
                    }
                }
            }
            Some(Performative::Flow(ref flow)) => {
                trace!("Received flow!");
                // Session flow control
                {
                    let mut control = self.flow_control.lock().unwrap();
                    control.next_incoming_id = flow.next_outgoing_id;
                    control.remote_outgoing_window = flow.outgoing_window;
                    if let Some(next_incoming_id) = flow.next_incoming_id {
                        control.remote_incoming_window =
                            next_incoming_id + flow.incoming_window - control.next_outgoing_id;
                    } else {
                        control.remote_incoming_window = self.initial_outgoing_id
                            + flow.incoming_window
                            - control.next_outgoing_id;
                    }
                }
                if let Some(handle) = flow.handle {
                    let link = {
                        let mut m = self.links.lock().unwrap();
                        m.get_mut(&handle).ok_or(AmqpError::InvalidHandle)?.clone()
                    };
                    if let Some(credit) = flow.link_credit {
                        let credit = flow.delivery_count.unwrap_or(0) + credit
                            - link.delivery_count.load(Ordering::SeqCst);
                        link.credit.store(credit, Ordering::SeqCst);
                    }
                }
            }
            _ => {
                warn!("Unexpected performative for session: {:?}", frame);
            }
        }
        Ok(())
    }

    pub fn close(&self, error: Option<ErrorCondition>) -> Result<()> {
        self.connection.end(self.local_channel, End { error })
    }

    pub async fn new_link(
        &self,
        address: &str,
        options: impl Into<LinkOptions>,
    ) -> Result<(String, Arc<LinkDriver>)> {
        let options = options.into();
        let role = options.role();
        let link_name = format!("dove-{}-{}", Uuid::new_v4().to_string(), role.as_str());
        debug!("Creating link {} with role {:?}", link_name, role);

        // Send attach frame
        let attach = Attach {
            name: link_name.clone(),
            handle: self.next_handle_id(),
            role,
            snd_settle_mode: None,
            rcv_settle_mode: None,
            source: Some(Source {
                address: Some(address.to_string()),
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
                address: Some(address.to_string()),
                durable: None,
                expiry_policy: None,
                timeout: None,
                dynamic: Some(false),
                dynamic_node_properties: None,
                capabilities: None,
            }),
            unsettled: None,
            incomplete_unsettled: None,
            initial_delivery_count: if role == LinkRole::Sender {
                Some(0)
            } else {
                None
            },
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };

        let attach = options.applied_on_attach(attach);
        let link = Arc::new(LinkDriver {
            name: link_name.clone(),
            role,
            channel: self.local_channel,
            connection: self.connection.clone(),
            handle: attach.handle,
            rx: Channel::new(),
            session_flow_control: self.flow_control.clone(),
            did_to_delivery: self.did_to_delivery.clone(),
            credit: AtomicU32::new(0),
            delivery_count: AtomicU32::new(0),
        });

        self.links_in_flight
            .lock()
            .unwrap()
            .insert(link_name.clone(), Arc::clone(&link));

        debug!("Requesting attachment of {}/{}", attach.name, attach.handle);
        self.connection.attach(self.local_channel, attach)?;

        let frame = link.rx.recv().await?;
        if let Some(Performative::Attach(response)) = frame.performative {
            debug!(
                "Received response for attach request: handle={}",
                response.handle
            );

            // if it is not dynamic, we need to check whether the attach was successful
            let requested_address = address;
            let dynamic = matches!(options.dynamic(), Some(true));

            let address_response = match response.role {
                LinkRole::Sender => response.target.and_then(|t| t.address),
                LinkRole::Receiver => response.source.and_then(|s| s.address),
            };

            match address_response {
                Some(address) if dynamic || address == requested_address => Ok((address, link)),
                invalid => {
                    warn!(
                        "Expected address {:?}, but server sent {:?}",
                        requested_address, invalid
                    );
                    link.close(Some(ErrorCondition {
                        condition: "amqp:invalid-field".to_string(),
                        description: format!(
                            "Expected address {:?}, but server sent {:?}",
                            requested_address, invalid
                        ),
                    }))?;
                    Err(AmqpError::TargetNotRecognized(
                        requested_address.to_string(),
                    ))
                }
            }
        } else {
            let condition = ErrorCondition {
                condition: "amqp:precondition-failed".to_string(),
                description: format!("Expected attach frame, but got {:?}", frame),
            };
            link.close(Some(condition.clone()))?;
            Err(AmqpError::Amqp(condition))
        }
    }

    fn next_handle_id(&self) -> HandleId {
        loop {
            let handle_id = self.handle_generator.fetch_add(1, Ordering::SeqCst);

            loop {
                // try_lock to prevent deadlocks
                let links = match self.links.try_lock() {
                    Ok(links) => links,
                    Err(_) => continue,
                };

                // try_lock to prevent deadlocks
                let links_in_flight = match self.links_in_flight.try_lock() {
                    Ok(links_in_flight) => links_in_flight,
                    Err(_) => continue,
                };

                if !links.values().any(|l| l.handle == handle_id)
                    && !links_in_flight.values().any(|l| l.handle == handle_id)
                {
                    return handle_id;
                } else {
                    break;
                }
            }
        }
    }

    #[inline]
    pub async fn recv(&self) -> Result<AmqpFrame> {
        self.rx.recv().await
    }

    #[inline]
    pub fn unrecv(&self, frame: AmqpFrame) -> Result<()> {
        warn!("unrecv");
        self.rx.send(frame)
    }
}

impl LinkDriver {
    pub fn connection(&self) -> &ConnectionHandle {
        &self.connection
    }

    pub async fn send_message(
        &self,
        message: Message,
        settled: bool,
    ) -> Result<Arc<DeliveryDriver>> {
        let semaphore_fn = |x| {
            if x == 0 {
                Some(0)
            } else {
                Some(x - 1)
            }
        };

        // Link flow control
        if self
            .credit
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, semaphore_fn)
            == Ok(0)
        {
            return Err(AmqpError::NotEnoughCreditsToSend(Box::new(message)));
        }

        // Session flow control
        let next_outgoing_id;
        loop {
            let props = self.session_flow_control.lock().unwrap().next();
            if let Some(props) = props {
                next_outgoing_id = props.next_outgoing_id;
                break;
            }
            // std::thread::sleep(Duration::from_millis(500));
        }

        self.delivery_count.fetch_add(1, Ordering::SeqCst);
        let delivery_tag = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        let delivery = Arc::new(DeliveryDriver {
            message: Some(message),
            id: next_outgoing_id,
            tag: delivery_tag.clone(),
            state: None,
            remotely_settled: false,
            settled,
        });

        if !settled {
            self.did_to_delivery
                .lock()
                .unwrap()
                .insert(next_outgoing_id, (self.handle, delivery.clone()));
        }

        let transfer = Transfer {
            handle: self.handle,
            delivery_id: Some(next_outgoing_id),
            delivery_tag: Some(delivery_tag),
            message_format: Some(0),
            settled: Some(settled),
            more: Some(false),
            rcv_settle_mode: None,
            state: None,
            resume: None,
            aborted: None,
            batchable: None,
        };

        let mut msgbuf = Vec::new();
        if let Some(message) = delivery.message.as_ref() {
            message.encode(&mut msgbuf)?;
        }

        self.connection
            .transfer(self.channel, transfer, Some(msgbuf))?;

        Ok(delivery)
    }

    pub fn flow(&self, credit: u32) -> Result<()> {
        trace!("{}: issuing {} credits", self.handle, credit);
        self.credit.store(credit, Ordering::SeqCst);
        let props = { self.session_flow_control.lock().unwrap().clone() };
        self.connection.flow(
            self.channel,
            Flow {
                next_incoming_id: Some(props.next_incoming_id),
                incoming_window: props.incoming_window,
                next_outgoing_id: props.next_outgoing_id,
                outgoing_window: props.outgoing_window,
                handle: Some(self.handle as u32),
                delivery_count: Some(self.delivery_count.load(Ordering::SeqCst)),
                link_credit: Some(credit),
                available: None,
                drain: None,
                echo: None,
                properties: None,
            },
        )
    }

    pub fn close(&self, error: Option<ErrorCondition>) -> Result<()> {
        self.connection.detach(
            self.channel,
            Detach {
                handle: self.handle,
                closed: Some(true),
                error,
            },
        )
    }

    #[inline]
    pub async fn recv(&self) -> Result<AmqpFrame> {
        self.rx.recv().await
    }

    #[inline]
    pub fn unrecv(&self, frame: AmqpFrame) -> Result<()> {
        warn!("unrecv");
        self.rx.send(frame)
    }

    pub fn disposition(
        &self,
        delivery: &DeliveryDriver,
        settled: bool,
        state: DeliveryState,
    ) -> Result<()> {
        if settled {
            self.did_to_delivery.lock().unwrap().remove(&delivery.id);
            let mut control = self.session_flow_control.lock().unwrap();
            control.incoming_window += 1;
        }
        let disposition = framing::Disposition {
            role: self.role,
            first: delivery.id,
            last: Some(delivery.id),
            settled: Some(settled),
            state: Some(state),
            batchable: None,
        };

        self.connection().disposition(self.channel, disposition)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Channel<T> {
    tx: async_channel::Sender<T>,
    rx: async_channel::Receiver<T>,
}

impl<T> Default for Channel<T> {
    fn default() -> Self {
        let (tx, rx) = async_channel::unbounded();
        Self { tx, rx }
    }
}

impl<T> Channel<T> {
    pub fn new() -> Channel<T> {
        Self::default()
    }

    #[inline]
    pub fn send(&self, value: T) -> Result<()> {
        Ok(self.tx.try_send(value)?)
    }

    #[inline]
    pub fn try_recv(&self) -> Result<T> {
        Ok(self.rx.try_recv()?)
    }

    #[inline]
    pub async fn recv(&self) -> Result<T> {
        Ok(self.rx.recv().await?)
    }

    pub fn close(&self) {
        self.tx.close();
        self.rx.close();
    }

    pub fn handle<H: From<async_channel::Sender<T>>>(&self) -> H {
        H::from(self.tx.clone())
    }

    pub fn handle_with<P, H: From<(async_channel::Sender<T>, P)>>(&self, param: P) -> H {
        H::from((self.tx.clone(), param))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn check_handle_map() {}
}
