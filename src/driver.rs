/*
 * Copyright 2019-2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The driver module is an intermediate layer with the core logic for interacting with different AMQP 1.0 endpoint entities (connections, sessions, links).

use crate::conn;
use crate::conn::ChannelId;
use crate::error::*;
use crate::framing;
use crate::framing::{
    AmqpFrame, Attach, Begin, Close, DeliveryState, Detach, End, Flow, Frame, LinkRole,
    Performative, Source, Target, Transfer,
};
use crate::message::Message;
use crate::transport::MioNetwork;
use log::{trace, warn};
use mio::{Interest, Poll, Token};
use rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub type DeliveryTag = Vec<u8>;
pub type HandleId = u32;

#[derive(Debug)]
pub struct ConnectionDriver {
    channel_max: u16,
    idle_timeout: Duration,
    driver: Arc<Mutex<conn::Connection<MioNetwork>>>,
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
    driver: Arc<Mutex<conn::Connection<MioNetwork>>>,
    local_channel: ChannelId,
    rx: Channel<AmqpFrame>,
    links: Mutex<HashMap<HandleId, Arc<LinkDriver>>>,
    #[allow(clippy::type_complexity)]
    did_to_delivery: Arc<Mutex<HashMap<u32, (HandleId, Arc<DeliveryDriver>)>>>,
    handle_generator: AtomicU32,
    initial_outgoing_id: u32,

    flow_control: Arc<Mutex<SessionFlowControl>>,
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
            Err(AmqpError::framing_error())
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
    pub handle: u32,
    pub role: LinkRole,
    pub channel: ChannelId,
    driver: Arc<Mutex<conn::Connection<MioNetwork>>>,
    rx: Channel<AmqpFrame>,

    session_flow_control: Arc<Mutex<SessionFlowControl>>,

    #[allow(clippy::type_complexity)]
    did_to_delivery: Arc<Mutex<HashMap<u32, (HandleId, Arc<DeliveryDriver>)>>>,
    credit: AtomicU32,
    delivery_count: AtomicU32,
}

#[derive(Debug)]
pub struct DeliveryDriver {
    pub message: Message,
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
    pub fn new(conn: conn::Connection<MioNetwork>) -> ConnectionDriver {
        ConnectionDriver {
            driver: Arc::new(Mutex::new(conn)),
            rx: Channel::new(),
            sessions: Mutex::new(HashMap::new()),
            remote_channel_map: Mutex::new(HashMap::new()),
            idle_timeout: Duration::from_secs(5),
            remote_idle_timeout: Duration::from_secs(0),
            channel_max: std::u16::MAX,
            closed: AtomicBool::new(false),
        }
    }

    pub fn register(&self, id: Token, poll: &mut Poll) -> Result<()> {
        let mut d = self.driver.lock().unwrap();
        let network = d.transport().network();
        poll.registry()
            .register(&mut *network, id, Interest::READABLE | Interest::WRITABLE)?;
        Ok(())
    }

    pub fn driver(&self) -> std::sync::MutexGuard<conn::Connection<MioNetwork>> {
        self.driver.lock().unwrap()
    }

    pub fn flowcontrol(&self, connection: &mut conn::Connection<MioNetwork>) -> Result<()> {
        let low_flow_watermark = 100;
        let high_flow_watermark = 1000;

        for (_, session) in self.sessions.lock().unwrap().iter_mut() {
            for (_, link) in session.links.lock().unwrap().iter_mut() {
                if link.role == LinkRole::Receiver {
                    let credit = link.credit.load(Ordering::SeqCst);
                    if credit <= low_flow_watermark {
                        link.flowcontrol(high_flow_watermark, connection)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn keepalive(&self, connection: &mut conn::Connection<MioNetwork>) -> Result<()> {
        // Sent out keepalives...
        let now = Instant::now();

        let last_received = connection.keepalive(self.remote_idle_timeout, now)?;
        if self.idle_timeout.as_millis() > 0 {
            // Ensure our peer honors our keepalive
            if now - last_received > self.idle_timeout * 2 {
                connection.close(Close {
                    error: Some(ErrorCondition {
                        condition: condition::RESOURCE_LIMIT_EXCEEDED.to_string(),
                        description: "local-idle-timeout expired".to_string(),
                    }),
                })?;
            }
        }
        Ok(())
    }

    pub fn close(&self, error: Option<ErrorCondition>) -> Result<()> {
        if self.closed.fetch_or(true, Ordering::SeqCst) {
            return Ok(());
        }
        let mut driver = self.driver.lock().unwrap();
        driver.close(Close { error })?;
        driver.flush()?;
        driver.shutdown()?;
        Ok(())
    }

    pub fn process(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        // Read frames until we're blocked
        let mut rx_frames = Vec::new();
        {
            let mut driver = self.driver.lock().unwrap();
            loop {
                if self.closed.load(Ordering::SeqCst) {
                    return Ok(());
                }
                let result = driver.process(&mut rx_frames);
                match result {
                    Ok(_) => {}
                    // This means that we should poll again to await further I/O action for this driver.
                    Err(AmqpError::IoError(ref e))
                        if e.kind() == std::io::ErrorKind::WouldBlock =>
                    {
                        break;
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
        }

        if !rx_frames.is_empty() {
            trace!("Dispatching {:?} frames", rx_frames.len());
        }

        self.dispatch(rx_frames)
    }

    fn dispatch(&self, mut frames: Vec<Frame>) -> Result<()> {
        // Process received frames.
        for frame in frames.drain(..) {
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
                                }
                                s.rx.send(frame)?;
                            }
                        }
                        Performative::End(ref _end) => {
                            let mut m = self.sessions.lock().unwrap();
                            m.get_mut(&channel).map(|s| s.rx.send(frame));
                        }
                        _ => {
                            let session = {
                                let mut m = self.sessions.lock().unwrap();
                                m.get_mut(&channel).cloned()
                            };

                            if let Some(s) = session {
                                s.dispatch(frame)?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn allocate_session(&self, remote_channel_id: Option<ChannelId>) -> Option<Arc<SessionDriver>> {
        let mut m = self.sessions.lock().unwrap();
        for i in 0..self.channel_max {
            let chan = i as ChannelId;
            if !m.contains_key(&chan) {
                let session = Arc::new(SessionDriver {
                    driver: self.driver.clone(),
                    local_channel: chan,
                    rx: Channel::new(),
                    links: Mutex::new(HashMap::new()),
                    handle_generator: AtomicU32::new(0),
                    flow_control: Arc::new(Mutex::new(SessionFlowControl::new())),
                    initial_outgoing_id: 0,

                    did_to_delivery: Arc::new(Mutex::new(HashMap::new())),
                });
                m.insert(chan, session.clone());
                remote_channel_id.map(|c| self.remote_channel_map.lock().unwrap().insert(c, chan));
                return Some(session);
            }
        }
        None
    }

    pub async fn new_session(&self, _opts: Option<SessionOpts>) -> Result<Arc<SessionDriver>> {
        let session = self.allocate_session(None).unwrap();
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
        self.driver
            .lock()
            .unwrap()
            .begin(session.local_channel, begin)?;

        Ok(session)
    }

    pub fn recv(&self) -> Result<AmqpFrame> {
        self.rx.recv()
    }

    pub fn unrecv(&self, frame: AmqpFrame) -> Result<()> {
        self.rx.send(frame)
    }
}

impl SessionDriver {
    pub fn dispatch(&self, frame: AmqpFrame) -> Result<()> {
        match frame.performative {
            Some(Performative::Attach(ref _attach)) => {
                self.rx.send(frame)?;
            }
            Some(Performative::Detach(ref _detach)) => {
                self.rx.send(frame)?;
            }
            Some(Performative::Transfer(ref transfer)) => {
                // Session flow control
                if let Some(delivery_id) = transfer.delivery_id {
                    loop {
                        let result = self.flow_control.lock().unwrap().accept(delivery_id);
                        match result {
                            Err(AmqpError::Amqp(cond)) => {
                                self.close(Some(cond))?;
                            }
                            Err(_) => {
                                self.close(None)?;
                            }
                            Ok(true) => {
                                break;
                            }
                            _ => {}
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
                    if let Some((handle, _)) = self.did_to_delivery.lock().unwrap().get(&id) {
                        let link = {
                            let mut m = self.links.lock().unwrap();
                            m.get_mut(&handle).unwrap().clone()
                        };
                        if link.role == disposition.role {
                            link.rx.send(frame.clone())?;
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
                        m.get_mut(&handle).unwrap().clone()
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
        let mut driver = self.driver.lock().unwrap();
        driver.end(self.local_channel, End { error })?;
        driver.flush()
    }

    pub fn new_link(&self, addr: &str, role: LinkRole) -> Result<Arc<LinkDriver>> {
        trace!("Creating new link!");
        let handle = self.handle_generator.fetch_add(1, Ordering::SeqCst);
        let link = Arc::new(LinkDriver {
            role,
            channel: self.local_channel,
            driver: self.driver.clone(),
            handle,
            rx: Channel::new(),
            session_flow_control: self.flow_control.clone(),
            did_to_delivery: self.did_to_delivery.clone(),
            credit: AtomicU32::new(0),
            delivery_count: AtomicU32::new(0),
        });
        // TODO: Increment id
        {
            let mut m = self.links.lock().unwrap();
            m.insert(handle, link.clone());
        }

        // Send attach frame
        let attach = Attach {
            name: addr.to_string(),
            handle: handle as u32,
            role,
            snd_settle_mode: None,
            rcv_settle_mode: None,
            source: Some(Source {
                address: Some(addr.to_string()),
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
                address: Some(addr.to_string()),
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
        self.driver
            .lock()
            .unwrap()
            .attach(self.local_channel, attach)?;
        Ok(link)
    }

    pub fn recv(&self) -> Result<AmqpFrame> {
        self.rx.recv()
    }

    pub fn unrecv(&self, frame: AmqpFrame) -> Result<()> {
        self.rx.send(frame)
    }
}

impl LinkDriver {
    pub fn driver(&self) -> std::sync::MutexGuard<conn::Connection<MioNetwork>> {
        self.driver.lock().unwrap()
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
        while self
            .credit
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, semaphore_fn)
            == Ok(0)
        {
            std::thread::sleep(Duration::from_millis(500));
            /*
            return Err(AmqpError::amqp_error(
                "not enough available credits to send message",
                None,
            ));
            */
        }

        // Session flow control
        let next_outgoing_id;
        loop {
            let props = self.session_flow_control.lock().unwrap().next();
            if let Some(props) = props {
                next_outgoing_id = props.next_outgoing_id;
                break;
            }
            std::thread::sleep(Duration::from_millis(500));
        }

        self.delivery_count.fetch_add(1, Ordering::SeqCst);
        let delivery_tag = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        let delivery = Arc::new(DeliveryDriver {
            message,
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
        delivery.message.encode(&mut msgbuf)?;

        self.driver
            .lock()
            .unwrap()
            .transfer(self.channel, transfer, Some(msgbuf))?;

        Ok(delivery)
    }

    pub async fn flow(&self, credit: u32) -> Result<()> {
        let mut driver = self.driver.lock().unwrap();
        self.flowcontrol(credit, &mut driver)
    }

    fn flowcontrol(
        &self,
        credit: u32,
        connection: &mut conn::Connection<MioNetwork>,
    ) -> Result<()> {
        trace!("{}: issuing {} credits", self.handle, credit);
        self.credit.store(credit, Ordering::SeqCst);
        let props = { self.session_flow_control.lock().unwrap().clone() };
        connection.flow(
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
        let mut driver = self.driver.lock().unwrap();
        driver.detach(
            self.channel,
            Detach {
                handle: self.handle,
                closed: Some(true),
                error,
            },
        )?;
        driver.flush()
    }

    pub fn recv(&self) -> Result<AmqpFrame> {
        self.rx.recv()
    }

    pub fn unrecv(&self, frame: AmqpFrame) -> Result<()> {
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

        self.driver().disposition(self.channel, disposition)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Channel<T> {
    tx: Mutex<mpsc::Sender<T>>,
    rx: Mutex<mpsc::Receiver<T>>,
}

impl<T> Channel<T> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Channel<T> {
        let (tx, rx) = mpsc::channel();
        Channel {
            tx: Mutex::new(tx),
            rx: Mutex::new(rx),
        }
    }

    pub fn send(&self, value: T) -> Result<()> {
        self.tx.lock().unwrap().send(value)?;
        Ok(())
    }

    pub fn try_recv(&self) -> Result<T> {
        let r = self.rx.lock().unwrap().try_recv()?;
        Ok(r)
    }

    pub fn recv(&self) -> Result<T> {
        let r = self.rx.lock().unwrap().recv()?;
        Ok(r)
    }
}
