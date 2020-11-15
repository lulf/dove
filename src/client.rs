/*
 * Copyright 2019-2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The client module contains a simple client API for sending and receiving messages.
use crate::conn;
use crate::conn::{ChannelId, ConnectionOptions};
use crate::error::*;
use crate::framing::{
    AmqpFrame, Attach, Begin, Close, DeliveryState, Flow, Frame, LinkRole, Open, Performative,
    Source, Target, Transfer,
};
use crate::message::{Message, MessageProperties};
use crate::types::Value;
use log::trace;
// use mio::event;
use mio::{Events, Interest, Poll, Token};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
// use crate::message::*;
use rand::Rng;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

pub type DeliveryTag = Vec<u8>;
type HandleId = u32;

pub struct Client {
    container_id: String,
    poll: Mutex<Poll>,
    incoming: Channel<Token>,
    connections: Mutex<HashMap<Token, Arc<ConnectionInner>>>,
    token_generator: AtomicU32,
}

pub struct Connection {
    connection: Arc<ConnectionInner>,

    pub container_id: String,
    pub hostname: String,
    pub channel_max: u16,
    pub idle_timeout: Duration,

    pub remote_idle_timeout: Duration,
    pub remote_container_id: String,
    pub remote_channel_max: u16,
}

struct ConnectionInner {
    channel_max: u16,
    idle_timeout: Duration,
    driver: Arc<Mutex<conn::Connection>>,
    sessions: Mutex<HashMap<ChannelId, Arc<SessionInner>>>,

    // Frames received on this connection
    rx: Channel<AmqpFrame>,
    remote_channel_map: Mutex<HashMap<ChannelId, ChannelId>>,
    remote_idle_timeout: Duration,
}

pub struct Session {
    connection: Arc<ConnectionInner>,
    session: Arc<SessionInner>,
}

struct SessionInner {
    // Frames received on this session
    driver: Arc<Mutex<conn::Connection>>,
    local_channel: ChannelId,
    rx: Channel<AmqpFrame>,
    links: Mutex<HashMap<HandleId, Arc<LinkInner>>>,
    handle_generator: AtomicU32,
    did_generator: Arc<AtomicU32>,
    did_to_link: Arc<Mutex<HashMap<u32, HandleId>>>,
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

pub struct Sender {
    handle: u32,
    connection: Arc<ConnectionInner>,
    link: Arc<LinkInner>,
    next_message_id: AtomicU64,
    /*
    driver: Arc<Mutex<conn::Connection>>,
    channel: ChannelId,
    handle: HandleId,
    opened: Channel<()>,
    */
}

pub struct Receiver {
    handle: u32,
    connection: Arc<ConnectionInner>,
    link: Arc<LinkInner>,
    next_message_id: AtomicU64,
    /*
    driver: Arc<Mutex<conn::Connection>>,
    channel: ChannelId,
    handle: HandleId,
    opened: Channel<()>,
    */
}

struct LinkInner {
    handle: u32,
    role: LinkRole,
    channel: ChannelId,
    driver: Arc<Mutex<conn::Connection>>,
    rx: Channel<AmqpFrame>,
    did_generator: Arc<AtomicU32>,
    did_to_link: Arc<Mutex<HashMap<u32, HandleId>>>,
    unsettled: Mutex<HashMap<DeliveryTag, Arc<DeliveryInner>>>,
}

pub struct Disposition {
    delivery: Arc<DeliveryInner>, /*    pub remote_settled: bool,
                                  pub state: Option<DeliveryState>,*/
}

pub struct DeliveryInner {
    message: Message,
    remotely_settled: bool,
    settled: bool,
    state: Option<DeliveryState>,
    tag: DeliveryTag,
    id: u32,
}

pub struct SessionOpts {
    pub max_frame_size: u32,
}

impl Client {
    pub fn new() -> Result<Client> {
        let p = Poll::new()?;
        Ok(Client {
            container_id: "rs-amqp10".to_string(),
            incoming: Channel::new(),
            poll: Mutex::new(p),
            connections: Mutex::new(HashMap::new()),
            token_generator: AtomicU32::new(0),
        })
    }

    pub async fn connect(
        &self,
        host: &str,
        port: u16,
        opts: ConnectionOptions,
    ) -> Result<Connection> {
        let mut driver = conn::connect(host, port, opts)?;
        println!("Connected! Sending open...");

        let mut open = Open::new(self.container_id.as_str());
        open.hostname = Some(host.to_string());
        open.channel_max = Some(std::u16::MAX);
        open.idle_timeout = Some(5000);
        driver.open(open)?;

        let conn = Arc::new(ConnectionInner {
            driver: Arc::new(Mutex::new(driver)),
            rx: Channel::new(),
            sessions: Mutex::new(HashMap::new()),
            remote_channel_map: Mutex::new(HashMap::new()),
            idle_timeout: Duration::from_secs(5),
            remote_idle_timeout: Duration::from_secs(0),
            channel_max: std::u16::MAX,
        });

        // TODO: Increment
        let id = Token(self.token_generator.fetch_add(1, Ordering::SeqCst) as usize);
        {
            let mut m = self.connections.lock().unwrap();
            m.insert(id, conn.clone());
        }
        self.incoming.send(id)?;

        println!("Waiting until opened...");
        loop {
            let frame = conn.rx.recv()?;
            match frame.performative {
                Some(Performative::Open(o)) => {
                    // Populate remote properties
                    return Ok(Connection {
                        connection: conn.clone(),
                        container_id: self.container_id.clone(),
                        hostname: host.to_string(),
                        channel_max: std::u16::MAX,
                        idle_timeout: Duration::from_secs(5),

                        remote_container_id: o.container_id.clone(),
                        remote_channel_max: o.channel_max.unwrap_or(std::u16::MAX),
                        remote_idle_timeout: Duration::from_millis(
                            o.idle_timeout.unwrap_or(0) as u64
                        ),
                    });
                }
                _ => {
                    // Push it back into the queue
                    // TODO: Prevent reordering
                    conn.rx.send(frame)?;
                }
            }
        }
    }

    pub fn process(&self) -> Result<()> {
        let mut events = Events::with_capacity(1024);

        let mut tokens = HashSet::new();

        // Register new connections
        loop {
            let result = self.incoming.try_recv();
            match result {
                Err(_) => break,
                Ok(id) => {
                    let mut m = self.connections.lock().unwrap();
                    let conn = m.get_mut(&id).unwrap();
                    let mut driver = conn.driver.lock().unwrap();
                    self.poll.lock().unwrap().registry().register(
                        &mut *driver,
                        id,
                        Interest::READABLE | Interest::WRITABLE,
                    )?;
                    tokens.insert(id);
                }
            }
        }

        // Push connection frames on the wire
        for (id, connection) in self.connections.lock().unwrap().iter_mut() {
            let mut driver = connection.driver.lock().unwrap();

            // Handle keepalive
            connection.keepalive(&mut driver);

            // Flush data
            println!("Flushing driver");
            let result = driver.flush();
            match result {
                Err(_) => {
                    println!("Error flushing connection {:?}", id);
                }
                _ => {}
            }
        }

        // Poll for new events
        {
            self.poll
                .lock()
                .unwrap()
                .poll(&mut events, Some(Duration::from_millis(100)))?;
        }

        for event in &events {
            let id = event.token();
            println!("Got event for {:?}", id);
            let connection = {
                let m = self.connections.lock().unwrap();
                m.get(&id).map(|c| c.clone())
            };
            match connection {
                Some(c) => c.process()?,
                _ => {}
            }
        }
        Ok(())
    }
}

impl ConnectionInner {
    fn keepalive(&self, connection: &mut conn::Connection) -> Result<()> {
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

    fn process(&self) -> Result<()> {
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

        println!("Got {:?} frames", rx_frames.len());

        self.dispatch(rx_frames)
    }

    fn dispatch(&self, mut frames: Vec<Frame>) -> Result<()> {
        // Process received frames.
        for frame in frames.drain(..) {
            if let Frame::AMQP(frame) = frame {
                println!("Got AMQP frame: {:?}", frame.performative);
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
    ) -> Option<Arc<SessionInner>> {
        let mut m = self.sessions.lock().unwrap();
        for i in 0..self.channel_max {
            let chan = i as ChannelId;
            if !m.contains_key(&chan) {
                let session = Arc::new(SessionInner {
                    driver: self.driver.clone(),
                    local_channel: chan,
                    rx: Channel::new(),
                    links: Mutex::new(HashMap::new()),
                    handle_generator: AtomicU32::new(0),
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

    pub async fn new_session(&self, _opts: Option<SessionOpts>) -> Result<Arc<SessionInner>> {
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
}

impl Connection {
    pub async fn new_session(&self, opts: Option<SessionOpts>) -> Result<Session> {
        let s = self.connection.new_session(opts).await?;

        println!("Waiting until begin...");
        loop {
            let frame = s.rx.recv()?;
            match frame.performative {
                Some(Performative::Begin(_b)) => {
                    // Populate remote properties
                    return Ok(Session {
                        connection: self.connection.clone(),
                        session: s,
                    });
                }
                _ => {
                    // Push it back into the queue
                    // TODO: Prevent reordering
                    s.rx.send(frame)?;
                }
            }
        }
    }
}

impl SessionInner {
    pub fn dispatch(&self, frame: AmqpFrame) -> Result<()> {
        match frame.performative {
            Some(Performative::Attach(ref _attach)) => {
                self.rx.send(frame)?;
            }
            Some(Performative::Detach(ref _detach)) => {
                self.rx.send(frame)?;
            }
            Some(Performative::Transfer(_transfer)) => {}
            Some(Performative::Disposition(ref disposition)) => {
                println!("Received disposition: {:?}", disposition);
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
            Some(Performative::Flow(ref _flow)) => {
                println!("Received flow!");
            }
            _ => {
                println!("Unexpected performative for session: {:?}", frame);
            }
        }
        Ok(())
    }

    pub fn new_link(&self, addr: &str, role: LinkRole) -> Result<Arc<LinkInner>> {
        println!("Creating new link!");
        let handle = self.handle_generator.fetch_add(1, Ordering::SeqCst);
        let link = Arc::new(LinkInner {
            role: role,
            channel: self.local_channel,
            driver: self.driver.clone(),
            handle: handle,
            rx: Channel::new(),
            unsettled: Mutex::new(HashMap::new()),
            did_generator: self.did_generator.clone(),
            did_to_link: self.did_to_link.clone(),
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
}

impl Session {
    pub async fn new_sender(&self, addr: &str) -> Result<Sender> {
        let link = self.session.new_link(addr, LinkRole::Sender)?;
        println!("Created link, waiting for attach frame");
        loop {
            let frame = self.session.rx.recv()?;
            match frame.performative {
                Some(Performative::Attach(_a)) => {
                    // Populate remote properties
                    return Ok(Sender {
                        handle: link.handle,
                        connection: self.connection.clone(),
                        link: link,
                        next_message_id: AtomicU64::new(0),
                    });
                }
                _ => {
                    // Push it back into the queue
                    // TODO: Prevent reordering
                    self.session.rx.send(frame)?;
                }
            }
        }
    }

    pub async fn new_receiver(&self, addr: &str) -> Result<Receiver> {
        let link = self.session.new_link(addr, LinkRole::Receiver)?;
        println!("Created link, waiting for attach frame");
        loop {
            let frame = self.session.rx.recv()?;
            match frame.performative {
                Some(Performative::Attach(_a)) => {
                    // Send initial flow
                    let flow = Flow {
                        next_incoming_id: None,
                        incoming_window: std::i32::MAX as u32,
                        next_outgoing_id: self.session.did_generator.load(Ordering::SeqCst),
                        outgoing_window: std::i32::MAX as u32,
                        handle: Some(link.handle as u32),
                        delivery_count: None,
                        link_credit: Some(10),
                        available: None,
                        drain: None,
                        echo: None,
                        properties: None,
                    };
                    self.session
                        .driver
                        .lock()
                        .unwrap()
                        .flow(self.session.local_channel, flow)?;

                    // Populate remote properties
                    return Ok(Receiver {
                        handle: link.handle,
                        connection: self.connection.clone(),
                        link: link,
                        next_message_id: AtomicU64::new(0),
                    });
                }
                _ => {
                    // Push it back into the queue
                    // TODO: Prevent reordering
                    self.session.rx.send(frame)?;
                }
            }
        }
    }
}

impl LinkInner {
    pub async fn send(&self, message: Message, settled: bool) -> Result<Arc<DeliveryInner>> {
        let delivery_tag = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        let delivery_id = self.did_generator.fetch_add(1, Ordering::SeqCst);
        let delivery = Arc::new(DeliveryInner {
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
}

impl Sender {
    pub async fn send(&self, data: &str) -> Result<Disposition> {
        let mut message = Message::amqp_value(Value::String(data.to_string()));
        message.properties = Some(MessageProperties {
            message_id: Some(Value::Ulong(
                self.next_message_id.fetch_add(1, Ordering::SeqCst),
            )),
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
        let settled = false;
        let delivery = self.link.send(message, settled).await?;

        if !settled {
            loop {
                let frame = self.link.rx.recv()?;
                match frame.performative {
                    Some(Performative::Disposition(ref disposition)) => {
                        let first = disposition.first;
                        let last = disposition.last.unwrap_or(first);
                        if first <= delivery.id && last >= delivery.id {
                            // TODO: Better error checking
                            return Ok(Disposition { delivery: delivery });
                        } else {
                            self.link.rx.send(frame)?;
                        }
                    }
                    _ => {
                        // TODO: Prevent reordering
                        self.link.rx.send(frame)?;
                    }
                }
            }
        } else {
            return Ok(Disposition { delivery: delivery });
        }
    }
}

impl Receiver {
    pub async fn receive(&self) -> Result<Delivery> {
        loop {
            let frame = self.link.rx.recv()?;
            match frame.performative {
                Some(Performative::Transfer(ref transfer)) => {
                    println!("Got transfer!");
                    let mut input = frame.payload.unwrap();
                    let message = Message::decode(&mut input)?;
                    let delivery = Arc::new(DeliveryInner {
                        state: transfer.state.clone(),
                        tag: transfer.delivery_tag.clone().unwrap(),
                        id: transfer.delivery_id.unwrap(),
                        remotely_settled: transfer.settled.unwrap_or(false),
                        settled: false,
                        message: message,
                    });
                    return Ok(Delivery { delivery: delivery });
                }
                _ => {
                    // TODO: Prevent reordering
                    self.link.rx.send(frame)?;
                }
            }
        }
    }
}

pub struct Delivery {
    delivery: Arc<DeliveryInner>,
}

impl Delivery {
    pub fn message(&self) -> &Message {
        return &self.delivery.message;
    }
}

pub struct Channel<T> {
    tx: Mutex<mpsc::Sender<T>>,
    rx: Mutex<mpsc::Receiver<T>>,
}

impl<T> Channel<T> {
    fn new() -> Channel<T> {
        let (tx, rx) = mpsc::channel();
        return Channel {
            tx: Mutex::new(tx),
            rx: Mutex::new(rx),
        };
    }

    fn send(&self, value: T) -> Result<()> {
        self.tx.lock().unwrap().send(value)?;
        Ok(())
    }

    fn try_recv(&self) -> Result<T> {
        let r = self.rx.lock().unwrap().try_recv()?;
        Ok(r)
    }

    fn recv(&self) -> Result<T> {
        let r = self.rx.lock().unwrap().recv()?;
        Ok(r)
    }
}
