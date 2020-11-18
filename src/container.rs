/*
 * Copyright 2019-2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The container module contains a simple API for creating client connections and sending and receiving messages
use crate::conn;
use crate::driver::{
    Channel, ConnectionDriver, DeliveryDriver, LinkDriver, SessionDriver, SessionOpts,
};
use crate::error::*;
use crate::framing::{LinkRole, Open, Performative};

use log::trace;
use mio::{Events, Poll, Token, Waker};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use uuid::Uuid;

// Re-exports
pub use crate::conn::ConnectionOptions;
pub use crate::framing::DeliveryState;
pub use crate::message::{Message, MessageProperties};
pub use crate::sasl::SaslMechanism;
pub use crate::types::{Value, ValueRef};

pub struct Container {
    container: Arc<ContainerInner>,
    running: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

struct ContainerInner {
    container_id: String,
    poll: Mutex<Poll>,
    incoming: Channel<Token>,
    connections: Mutex<HashMap<Token, Arc<ConnectionDriver>>>,
    token_generator: AtomicU32,
    waker: Arc<Waker>,
}

pub struct Connection {
    connection: Arc<ConnectionDriver>,

    pub container_id: String,
    pub hostname: String,
    pub channel_max: u16,
    pub idle_timeout: Duration,

    pub remote_idle_timeout: Duration,
    pub remote_container_id: String,
    pub remote_channel_max: u16,
}

pub struct Session {
    connection: Arc<ConnectionDriver>,
    session: Arc<SessionDriver>,
}

#[allow(dead_code)]
pub struct Sender {
    handle: u32,
    connection: Arc<ConnectionDriver>,
    link: Arc<LinkDriver>,
    next_message_id: AtomicU64,
}

#[allow(dead_code)]
pub struct Receiver {
    handle: u32,
    connection: Arc<ConnectionDriver>,
    link: Arc<LinkDriver>,
    next_message_id: AtomicU64,
}

#[allow(dead_code)]
pub struct Disposition {
    delivery: Arc<DeliveryDriver>,
}

pub struct Delivery {
    link: Arc<LinkDriver>,
    delivery: Arc<DeliveryDriver>,
}

impl Container {
    pub fn new() -> Result<Container> {
        let p = Poll::new()?;
        let waker = Arc::new(Waker::new(p.registry(), Token(std::u32::MAX as usize))?);
        let uuid = Uuid::new_v4();
        let inner = ContainerInner {
            container_id: uuid.to_string(),
            incoming: Channel::new(),
            poll: Mutex::new(p),
            connections: Mutex::new(HashMap::new()),
            token_generator: AtomicU32::new(0),
            waker: waker,
        };
        Ok(Container {
            container: Arc::new(inner),
            running: Arc::new(AtomicBool::new(false)),
            thread: None,
        })
    }

    pub fn start(mut self) -> Self {
        self.running.store(true, Ordering::SeqCst);
        let running = self.running.clone();
        let inner = self.container.clone();
        self.thread = Some(thread::spawn(move || {
            Container::do_work(running, inner);
        }));
        self
    }

    pub fn run(&self) {
        Container::do_work(self.running.clone(), self.container.clone());
    }

    fn do_work(running: Arc<AtomicBool>, container: Arc<ContainerInner>) {
        loop {
            if !running.load(Ordering::SeqCst) {
                return;
            }
            match container.process() {
                Err(e) => {
                    trace!("error while processing: {:?}", e);
                }
                _ => {}
            }
        }
    }

    pub async fn connect(
        &self,
        host: &str,
        port: u16,
        opts: ConnectionOptions,
    ) -> Result<Connection> {
        self.container.connect(host, port, opts).await
    }

    pub fn close(&mut self) -> Result<()> {
        self.container.close()?;
        self.running.store(false, Ordering::SeqCst);
        let thread = self.thread.take();
        match thread {
            Some(t) => {
                t.join()?;
            }
            _ => {}
        }
        Ok(())
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        match self.close() {
            _ => {}
        }
    }
}

impl ContainerInner {
    pub fn close(&self) -> Result<()> {
        for (_id, connection) in self.connections.lock().unwrap().iter_mut() {
            connection.close(None)?;
        }
        Ok(())
    }

    pub async fn connect(
        &self,
        host: &str,
        port: u16,
        opts: ConnectionOptions,
    ) -> Result<Connection> {
        let mut driver = conn::connect(host, port, opts)?;
        trace!("Connected! Sending open...");

        let mut open = Open::new(self.container_id.as_str());
        open.hostname = Some(host.to_string());
        open.channel_max = Some(std::u16::MAX);
        open.idle_timeout = Some(5000);
        driver.open(open)?;

        // TODO: Increment
        let id = Token(self.token_generator.fetch_add(1, Ordering::SeqCst) as usize);
        let conn = {
            let conn = Arc::new(ConnectionDriver::new(driver, self.waker.clone()));
            let mut m = self.connections.lock().unwrap();
            m.insert(id, conn.clone());
            conn
        };
        self.incoming.send(id)?;
        self.waker.wake()?;

        trace!("Waiting until opened...");
        loop {
            let frame = conn.recv()?;
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
                    conn.unrecv(frame)?;
                }
            }
        }
    }

    pub fn process(&self) -> Result<()> {
        // Register new connections
        loop {
            let result = self.incoming.try_recv();
            match result {
                Err(_) => break,
                Ok(id) => {
                    let mut m = self.connections.lock().unwrap();
                    let conn = m.get_mut(&id).unwrap();
                    let mut poll = self.poll.lock().unwrap();
                    conn.register(id, &mut poll)?;
                }
            }
        }

        // Push connection frames on the wire
        for (id, connection) in self.connections.lock().unwrap().iter_mut() {
            let mut driver = connection.driver();

            // Handle keepalive
            connection.keepalive(&mut driver)?;

            connection.flowcontrol(&mut driver)?;

            // Flush data
            trace!("Flushing driver");
            let result = driver.flush();
            match result {
                Err(_) => {
                    trace!("Error flushing connection {:?}", id);
                }
                _ => {}
            }
        }

        // Poll for new events
        let mut events = Events::with_capacity(1024);
        {
            self.poll
                .lock()
                .unwrap()
                .poll(&mut events, Some(Duration::from_millis(2000)))?;
        }

        let waker_token = Token(std::u32::MAX as usize);
        for event in &events {
            let id = event.token();
            trace!("Got event for {:?}", id);
            if id == waker_token {
                for (_, connection) in self.connections.lock().unwrap().iter_mut() {
                    connection.process()?;
                }
            } else {
                let connection = {
                    let m = self.connections.lock().unwrap();
                    m.get(&id).map(|c| c.clone())
                };
                match connection {
                    Some(c) => c.process()?,
                    _ => {}
                }
            }
        }
        Ok(())
    }
}

impl Drop for ContainerInner {
    fn drop(&mut self) {
        match self.close() {
            _ => {}
        }
    }
}

impl Connection {
    pub async fn new_session(&self, opts: Option<SessionOpts>) -> Result<Session> {
        let s = self.connection.new_session(opts).await?;

        self.connection.wakeup()?;
        trace!("Waiting until begin...");
        loop {
            let frame = s.recv()?;
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
                    s.unrecv(frame)?;
                }
            }
        }
    }

    pub fn close(&self, error: Option<ErrorCondition>) -> Result<()> {
        self.connection.close(error)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        match self.close(None) {
            _ => {}
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        match self.close(None) {
            _ => {}
        }
    }
}

impl Session {
    pub async fn new_sender(&self, addr: &str) -> Result<Sender> {
        let link = self.session.new_link(addr, LinkRole::Sender)?;
        trace!("Created link, waiting for attach frame");
        self.connection.wakeup()?;
        loop {
            let frame = self.session.recv()?;
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
                    self.session.unrecv(frame)?;
                }
            }
        }
    }

    pub async fn new_receiver(&self, addr: &str) -> Result<Receiver> {
        let link = self.session.new_link(addr, LinkRole::Receiver)?;
        trace!("Created link, waiting for attach frame");
        self.connection.wakeup()?;
        loop {
            let frame = self.session.recv()?;
            match frame.performative {
                Some(Performative::Attach(_a)) => {
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
                    self.session.unrecv(frame)?;
                }
            }
        }
    }

    pub fn close(&self, error: Option<ErrorCondition>) -> Result<()> {
        self.session.close(error)
    }
}

impl Sender {
    pub async fn send(&self, mut message: Message) -> Result<Disposition> {
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
        let delivery = self.link.send_message(message, settled).await?;
        self.connection.wakeup()?;

        if !settled {
            loop {
                let frame = self.link.recv()?;
                match frame.performative {
                    Some(Performative::Disposition(ref disposition)) => {
                        let first = disposition.first;
                        let last = disposition.last.unwrap_or(first);
                        if first <= delivery.id && last >= delivery.id {
                            // TODO: Better error checking
                            return Ok(Disposition { delivery: delivery });
                        } else {
                            self.link.unrecv(frame)?;
                        }
                    }
                    _ => {
                        // TODO: Prevent reordering
                        self.link.unrecv(frame)?;
                    }
                }
            }
        } else {
            return Ok(Disposition { delivery: delivery });
        }
    }

    pub fn close(&self, error: Option<ErrorCondition>) -> Result<()> {
        self.link.close(error)
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        match self.close(None) {
            _ => {}
        }
    }
}

impl Receiver {
    pub async fn flow(&self, credit: u32) -> Result<()> {
        self.link.flow(credit).await?;
        self.connection.wakeup()?;
        Ok(())
    }

    pub async fn receive(&self) -> Result<Delivery> {
        loop {
            let frame = self.link.recv()?;
            match frame.performative {
                Some(Performative::Transfer(ref transfer)) => {
                    trace!("Got transfer!");
                    let mut input = frame.payload.unwrap();
                    let message = Message::decode(&mut input)?;
                    let delivery = Arc::new(DeliveryDriver {
                        state: transfer.state.clone(),
                        tag: transfer.delivery_tag.clone().unwrap(),
                        id: transfer.delivery_id.unwrap(),
                        remotely_settled: transfer.settled.unwrap_or(false),
                        settled: false,
                        message: message,
                    });
                    return Ok(Delivery {
                        link: self.link.clone(),
                        delivery: delivery,
                    });
                }
                _ => {
                    // TODO: Prevent reordering
                    self.link.unrecv(frame)?;
                }
            }
        }
    }

    pub fn close(&self, error: Option<ErrorCondition>) -> Result<()> {
        self.link.close(error)
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        match self.close(None) {
            _ => {}
        }
    }
}

impl Delivery {
    pub fn message(&self) -> &Message {
        return &self.delivery.message;
    }

    pub async fn disposition(&self, settled: bool, state: DeliveryState) -> Result<()> {
        self.link.disposition(&self.delivery, settled, state).await
    }
}
