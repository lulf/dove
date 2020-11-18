/*
 * Copyright 2019-2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use crate::conn;
use crate::conn::ChannelId;
use crate::error::*;
use crate::framing;
use crate::framing::{
    AmqpFrame, Attach, Begin, Close, DeliveryState, Detach, End, Flow, Frame, LinkRole,
    Performative, Source, Target, Transfer,
};
use crate::message::Message;
use log::trace;
use mio::{Interest, Poll, Token, Waker};
use rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub type DeliveryTag = Vec<u8>;
pub type HandleId = u32;

pub struct ConnectionDriver {
    channel_max: u16,
    idle_timeout: Duration,
    driver: Arc<Mutex<conn::Connection>>,
    sessions: Mutex<HashMap<ChannelId, Arc<SessionDriver>>>,
    waker: Arc<Waker>,

    // Frames received on this connection
    rx: Channel<AmqpFrame>,
    remote_channel_map: Mutex<HashMap<ChannelId, ChannelId>>,
    remote_idle_timeout: Duration,
}

pub struct SessionDriver {
    // Frames received on this session
    driver: Arc<Mutex<conn::Connection>>,
    local_channel: ChannelId,
    rx: Channel<AmqpFrame>,
    links: Mutex<HashMap<HandleId, Arc<LinkDriver>>>,
    handle_generator: AtomicU32,
    did_generator: Arc<AtomicU32>,
    did_to_link: Arc<Mutex<HashMap<u32, HandleId>>>,
    did_incoming_id: Arc<AtomicU32>,
    //   driver: Arc<Mutex<conn::Connection>>,
    /*    channel: ChannelId,
    driver: Arc<Mutex<conn::Connection>>,
    opened: Channel<()>,

    end_condition: Option<ErrorCondition>,
    remote_channel: Option<ChannelId>,
    handle_max: u32,
    delivery_to_handle: HashMap<u32, HandleId>,
    next_outgoing_id: u32,

    opts: SessionOpts,
    incoming: Channel<HandleId>,
    */
}

pub struct LinkDriver {
    pub handle: u32,
    pub role: LinkRole,
    pub channel: ChannelId,
    driver: Arc<Mutex<conn::Connection>>,
    rx: Channel<AmqpFrame>,
    did_generator: Arc<AtomicU32>,
    did_incoming_id: Arc<AtomicU32>,
    did_to_link: Arc<Mutex<HashMap<u32, HandleId>>>,
    unsettled: Mutex<HashMap<DeliveryTag, Arc<DeliveryDriver>>>,
    credit: AtomicU32,
    delivery_count: AtomicU32,
}

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
    pub fn new(conn: conn::Connection, waker: Arc<Waker>) -> ConnectionDriver {
        ConnectionDriver {
            driver: Arc::new(Mutex::new(conn)),
            rx: Channel::new(),
            sessions: Mutex::new(HashMap::new()),
            waker: waker,
            remote_channel_map: Mutex::new(HashMap::new()),
            idle_timeout: Duration::from_secs(5),
            remote_idle_timeout: Duration::from_secs(0),
            channel_max: std::u16::MAX,
        }
    }

    pub fn wakeup(&self) -> Result<()> {
        self.waker.wake()?;
        Ok(())
    }

    pub fn register(&self, id: Token, poll: &mut Poll) -> Result<()> {
        let mut d = self.driver.lock().unwrap();
        poll.registry()
            .register(&mut *d, id, Interest::READABLE | Interest::WRITABLE)?;
        Ok(())
    }

    pub fn driver(&self) -> std::sync::MutexGuard<conn::Connection> {
        self.driver.lock().unwrap()
    }

    pub fn flowcontrol(&self, connection: &mut conn::Connection) -> Result<()> {
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

    pub fn keepalive(&self, connection: &mut conn::Connection) -> Result<()> {
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
        let mut driver = self.driver.lock().unwrap();
        driver.close(Close { error: error })?;
        driver.flush()?;
        driver.shutdown()
    }

    pub fn process(&self) -> Result<()> {
        // Read frames until we're blocked
        let mut rx_frames = Vec::new();
        {
            let mut driver = self.driver.lock().unwrap();
            loop {
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

        trace!("Got {:?} frames", rx_frames.len());

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
                        Performative::Begin(ref _begin) => {
                            let mut m = self.sessions.lock().unwrap();
                            m.get_mut(&channel).map(|s| s.rx.send(frame));
                        }
                        Performative::End(ref _end) => {
                            let mut m = self.sessions.lock().unwrap();
                            m.get_mut(&channel).map(|s| s.rx.send(frame));
                        }
                        _ => {
                            let session = {
                                let mut m = self.sessions.lock().unwrap();
                                m.get_mut(&channel).map(|s| s.clone())
                            };

                            match session {
                                Some(s) => {
                                    s.dispatch(frame)?;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn allocate_session(
        self: &Self,
        remote_channel_id: Option<ChannelId>,
    ) -> Option<Arc<SessionDriver>> {
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
                    did_incoming_id: Arc::new(AtomicU32::new(0)),
                    did_generator: Arc::new(AtomicU32::new(0)),
                    did_to_link: Arc::new(Mutex::new(HashMap::new())),
                    //             driver: self.inner.clone(),
                    /*
                    remote_channel: remote_channel_id,
                    local_channel: chan,
                    handle_max: std::u32::MAX,
                    delivery_to_handle: HashMap::new(),
                    next_outgoing_id: 0,

                    opts: None,
                    incoming: Channel::new(),
                    opened: Channel::new(),
                    */
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
        let begin = Begin {
            remote_channel: None,
            next_outgoing_id: 0,
            incoming_window: 10,
            outgoing_window: 10,
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
            Some(Performative::Attach(ref attach)) => {
                if let Some(delivery_count) = attach.initial_delivery_count {
                    let link = {
                        let mut m = self.links.lock().unwrap();
                        m.get_mut(&attach.handle).unwrap().clone()
                    };
                    link.delivery_count.store(delivery_count, Ordering::SeqCst);
                }
                self.rx.send(frame)?;
            }
            Some(Performative::Detach(ref _detach)) => {
                self.rx.send(frame)?;
            }
            Some(Performative::Transfer(ref transfer)) => {
                let link = {
                    let mut m = self.links.lock().unwrap();
                    m.get_mut(&transfer.handle).unwrap().clone()
                };

                if link
                    .credit
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
                        if x <= 0 {
                            Some(0)
                        } else {
                            Some(x - 1)
                        }
                    })
                    == Ok(0)
                {
                    trace!("Transfer but no space left!");
                } else {
                    trace!(
                        "Received transfert. Credit: {:?}",
                        link.credit.load(Ordering::SeqCst)
                    );
                    if let Some(did) = transfer.delivery_id {
                        self.did_incoming_id.store(did + 1, Ordering::SeqCst);
                    }
                    link.delivery_count.fetch_add(1, Ordering::SeqCst);
                    link.rx.send(frame)?;
                }
            }
            Some(Performative::Disposition(ref disposition)) => {
                trace!("Received disposition: {:?}", disposition);
                let last = disposition.last.unwrap_or(disposition.first);
                for id in disposition.first..=last {
                    if let Some(handle) = self.did_to_link.lock().unwrap().get(&id) {
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
                if let Some(handle) = flow.handle {
                    let link = {
                        let mut m = self.links.lock().unwrap();
                        m.get_mut(&handle).unwrap().clone()
                    };
                    if let Some(credit) = flow.link_credit {
                        link.credit.store(credit, Ordering::SeqCst);
                    }
                }
            }
            _ => {
                trace!("Unexpected performative for session: {:?}", frame);
            }
        }
        Ok(())
    }

    pub fn close(&self, error: Option<ErrorCondition>) -> Result<()> {
        let mut driver = self.driver.lock().unwrap();
        driver.end(self.local_channel, End { error: error })?;
        driver.flush()
    }

    pub fn new_link(&self, addr: &str, role: LinkRole) -> Result<Arc<LinkDriver>> {
        trace!("Creating new link!");
        let handle = self.handle_generator.fetch_add(1, Ordering::SeqCst);
        let link = Arc::new(LinkDriver {
            role: role,
            channel: self.local_channel,
            driver: self.driver.clone(),
            handle: handle,
            rx: Channel::new(),
            unsettled: Mutex::new(HashMap::new()),
            did_generator: self.did_generator.clone(),
            did_to_link: self.did_to_link.clone(),
            did_incoming_id: self.did_incoming_id.clone(),
            credit: AtomicU32::new(0),
            delivery_count: AtomicU32::new(0),
        });
        // TODO: Increment id
        let mut m = self.links.lock().unwrap();
        m.insert(handle, link.clone());

        // Send attach frame
        let attach = Attach {
            name: addr.to_string(),
            handle: handle as u32,
            role: role,
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
    pub fn driver(&self) -> std::sync::MutexGuard<conn::Connection> {
        self.driver.lock().unwrap()
    }
    pub async fn send_message(
        &self,
        message: Message,
        settled: bool,
    ) -> Result<Arc<DeliveryDriver>> {
        while self
            .credit
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
                if x <= 0 {
                    Some(0)
                } else {
                    Some(x - 1)
                }
            })
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

        let delivery_tag = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        let delivery_id = self.did_generator.fetch_add(1, Ordering::SeqCst);
        let delivery = Arc::new(DeliveryDriver {
            message: message,
            id: delivery_id,
            tag: delivery_tag.clone(),
            state: None,
            remotely_settled: false,
            settled: settled,
        });

        if !settled {
            self.unsettled
                .lock()
                .unwrap()
                .insert(delivery_tag.clone(), delivery.clone());

            self.did_to_link
                .lock()
                .unwrap()
                .insert(delivery_id, self.handle);
        }

        let transfer = Transfer {
            handle: self.handle,
            delivery_id: Some(delivery_id),
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

    fn flowcontrol(&self, credit: u32, connection: &mut conn::Connection) -> Result<()> {
        self.credit.store(credit, Ordering::SeqCst);
        connection.flow(
            self.channel,
            Flow {
                next_incoming_id: Some(self.did_incoming_id.load(Ordering::SeqCst)),
                incoming_window: std::i32::MAX as u32,
                next_outgoing_id: self.did_generator.load(Ordering::SeqCst),
                outgoing_window: std::i32::MAX as u32,
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
                error: error,
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

    pub async fn disposition(
        &self,
        delivery: &DeliveryDriver,
        settled: bool,
        state: DeliveryState,
    ) -> Result<()> {
        if settled {
            self.unsettled.lock().unwrap().remove(&delivery.tag);
            self.did_to_link.lock().unwrap().remove(&delivery.id);
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

pub struct Channel<T> {
    tx: Mutex<mpsc::Sender<T>>,
    rx: Mutex<mpsc::Receiver<T>>,
}

impl<T> Channel<T> {
    pub fn new() -> Channel<T> {
        let (tx, rx) = mpsc::channel();
        return Channel {
            tx: Mutex::new(tx),
            rx: Mutex::new(rx),
        };
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
