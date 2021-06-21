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
use crate::framing::{AmqpFrame, Close, LinkRole, Open, Performative};
use crate::transport;

use log::{error, trace};
use mio::{Events, Poll, Token, Waker};
use std::cell::RefCell;
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
use crate::options::{LinkOptions, ReceiverOptions};
pub use crate::sasl::SaslMechanism;
pub use crate::types::{Value, ValueRef};

/// Represents an AMQP 1.0 container that can manage multiple connections.
pub struct Container {
    container: Arc<ContainerInner>,
    running: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

struct ContainerInner {
    container_id: String,
    poll: RefCell<Poll>,
    incoming: Channel<Token>,
    connections: Mutex<HashMap<Token, Arc<ConnectionDriver>>>,
    token_generator: AtomicU32,
    waker: Arc<Waker>,
    closed: AtomicBool,
}

/// Represents a single AMQP connection to a remote endpoint.
pub struct Connection {
    connection: Arc<ConnectionDriver>,
    waker: Arc<Waker>,

    pub container_id: String,
    pub hostname: String,
    pub channel_max: u16,
    pub idle_timeout: Duration,

    pub remote_idle_timeout: Duration,
    pub remote_container_id: String,
    pub remote_channel_max: u16,
}

/// Represents an AMQP session.
pub struct Session {
    waker: Arc<Waker>,
    connection: Arc<ConnectionDriver>,
    session: Arc<SessionDriver>,
}

/// Represents a sender link.
#[allow(dead_code)]
pub struct Sender {
    handle: u32,
    waker: Arc<Waker>,
    connection: Arc<ConnectionDriver>,
    link: Arc<LinkDriver>,
    next_message_id: AtomicU64,
}

/// Represents a receiver link.
#[allow(dead_code)]
pub struct Receiver {
    waker: Arc<Waker>,
    handle: u32,
    connection: Arc<ConnectionDriver>,
    link: Arc<LinkDriver>,
    next_message_id: AtomicU64,
}

/// Represents a disposition response for a sent message.
#[allow(dead_code)]
pub struct Disposition {
    delivery: Arc<DeliveryDriver>,
}

/// Represent a delivery
pub struct Delivery {
    settled: bool,
    message: Option<Message>,
    waker: Arc<Waker>,
    link: Arc<LinkDriver>,
    delivery: Arc<DeliveryDriver>,
}

unsafe impl std::marker::Sync for ContainerInner {}

impl Container {
    /// Creates a new container that can be used to connect to AMQP endpoints.
    /// use the start() method to launch a worker thread that handles the connection processing,
    /// or invoke the run() method.
    pub fn new() -> Result<Container> {
        Container::with_id(&Uuid::new_v4().to_string())
    }

    pub fn with_id(container_id: &str) -> Result<Container> {
        let p = Poll::new()?;
        let waker = Arc::new(Waker::new(p.registry(), Token(std::u32::MAX as usize))?);
        let inner = ContainerInner {
            container_id: container_id.to_string(),
            incoming: Channel::new(),
            poll: RefCell::new(p),
            connections: Mutex::new(HashMap::new()),
            token_generator: AtomicU32::new(0),
            waker,
            closed: AtomicBool::new(false),
        };
        Ok(Container {
            container: Arc::new(inner),
            running: Arc::new(AtomicBool::new(false)),
            thread: None,
        })
    }

    /// Start a worker thread to process connections for this container.
    pub fn start(mut self) -> Self {
        self.running.store(true, Ordering::SeqCst);
        let running = self.running.clone();
        let inner = self.container.clone();
        self.thread = Some(thread::spawn(move || {
            Container::do_work(running, inner);
        }));
        self
    }

    /// Process connections for this container.
    pub fn run(&self) {
        self.running.store(true, Ordering::SeqCst);
        Container::do_work(self.running.clone(), self.container.clone());
    }

    fn do_work(running: Arc<AtomicBool>, container: Arc<ContainerInner>) {
        log::debug!("Starting container processing loop");
        loop {
            if !running.load(Ordering::SeqCst) {
                log::debug!("Stopping container processing loop");
                return;
            }
            if let Err(e) = container.process() {
                error!(
                    "{}: error while processing: {:?}",
                    container.container_id, e
                );
            }
        }
    }

    /// Connect to an AMQP endpoint and send the initial open performative.
    pub async fn connect(
        &self,
        host: &str,
        port: u16,
        opts: ConnectionOptions,
    ) -> Result<Connection> {
        self.container.connect(host, port, opts).await
    }

    /// Close the connection. Flushes outgoing buffer before sending the final close performative,
    /// and closing the connection.
    pub fn close(&mut self) -> Result<()> {
        self.container.close()?;
        self.running.store(false, Ordering::SeqCst);
        let thread = self.thread.take();
        if let Some(t) = thread {
            t.join()?;
        }
        Ok(())
    }

    pub fn container_id(&self) -> &str {
        &self.container.container_id
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl ContainerInner {
    fn close(&self) -> Result<()> {
        if self.closed.fetch_or(true, Ordering::SeqCst) {
            return Ok(());
        }
        trace!("{}: shutting down container", self.container_id);

        for (_id, connection) in self.connections.lock().unwrap().drain() {
            connection.close(None)?;
        }
        self.waker.wake()?;
        trace!("{}: container is shut down", self.container_id);
        Ok(())
    }

    async fn connect(&self, host: &str, port: u16, opts: ConnectionOptions) -> Result<Connection> {
        let network = transport::mio::MioNetwork::connect(host, port)?;
        let transport = transport::Transport::new(network, 1024);
        let mut driver = conn::connect(transport, opts)?;
        trace!("{}: connected to {}:{}", self.container_id, host, port);

        let mut open = Open::new(self.container_id.as_str());
        open.hostname = Some(host.to_string());
        open.channel_max = Some(std::u16::MAX);
        open.idle_timeout = Some(5000);
        driver.open(open)?;

        let id = Token(self.token_generator.fetch_add(1, Ordering::SeqCst) as usize);
        log::debug!(
            "{}: created connection to {}:{} with local id {:?}",
            self.container_id,
            host,
            port,
            id,
        );
        let conn = {
            let conn = Arc::new(ConnectionDriver::new(driver));
            let mut m = self.connections.lock().unwrap();
            m.insert(id, conn.clone());
            conn
        };
        self.incoming.send(id)?;
        self.waker.wake()?;

        loop {
            let frame = conn.recv()?;
            match frame.performative {
                Some(Performative::Open(o)) => {
                    trace!(
                        "{}: received OPEN frame from {}:{}",
                        self.container_id,
                        host,
                        port
                    );
                    // Populate remote properties
                    return Ok(Connection {
                        waker: self.waker.clone(),
                        connection: conn,
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
                Some(Performative::Close(c)) => {
                    trace!(
                        "{}: received CLOSE frame from {}:{}",
                        self.container_id,
                        host,
                        port
                    );
                    match c.error {
                        Some(e) => {
                            return Err(AmqpError::Amqp(e));
                        }
                        None => {
                            return Err(AmqpError::Generic("connection closed".to_string()));
                        }
                    }
                }
                _ => {
                    // Push it back into the queue
                    // TODO: Prevent reordering
                    conn.unrecv(frame)?;
                }
            }
        }
    }

    fn process(&self) -> Result<()> {
        let mut poll = self.poll.borrow_mut();
        // Register new connections
        loop {
            let result = self.incoming.try_recv();
            match result {
                Err(_) => break,
                Ok(id) => {
                    let mut m = self.connections.lock().unwrap();
                    let conn = m.get_mut(&id);
                    if let Some(conn) = conn {
                        conn.register(id, &mut poll)?;
                    }
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
            let result = driver.flush();
            if result.is_err() {
                trace!("{}: error flushing driver for {:?}", self.container_id, id);
            }
        }

        // Poll for new events
        let mut events = Events::with_capacity(1024);
        {
            poll.poll(&mut events, Some(Duration::from_millis(2000)))?;
        }

        let waker_token = Token(std::u32::MAX as usize);
        for event in &events {
            let id = event.token();
            if id == waker_token {
                let ids: Vec<Token> = self.connections.lock().unwrap().keys().cloned().collect();
                for id in ids.iter() {
                    self.process_connection(*id)?;
                }
            } else {
                self.process_connection(id)?;
            }
        }
        Ok(())
    }

    fn process_connection(&self, id: Token) -> Result<()> {
        let connection = {
            let mut m = self.connections.lock().unwrap();
            m.get_mut(&id).cloned()
        };
        if let Some(c) = connection {
            let result = c.process();
            match result {
                Err(AmqpError::Amqp(condition)) => {
                    self.close_connection(id, &c, Some(condition))?;
                }
                Err(_) => {
                    self.close_connection(id, &c, None)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn close_connection(
        &self,
        id: Token,
        connection: &ConnectionDriver,
        condition: Option<ErrorCondition>,
    ) -> Result<()> {
        trace!(
            "{}: closing connection and removing reference to {:?}: {:?}",
            self.container_id,
            id,
            condition
        );
        let result = connection.close(condition.clone());
        match result {
            // Socket error is ignored
            Err(AmqpError::IoError(_)) => {}
            Err(e) => return Err(e),
            _ => {}
        }
        connection.unrecv(AmqpFrame {
            channel: 0,
            performative: Some(Performative::Close(Close { error: condition })),
            payload: None,
        })?;
        self.connections.lock().unwrap().remove(&id);
        Ok(())
    }
}

impl Drop for ContainerInner {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl Connection {
    /// Create a new session over this connection. Returns a session once the other
    /// endpoint have confirmed the creation.
    pub async fn new_session(&self, opts: Option<SessionOpts>) -> Result<Session> {
        let s = self.connection.new_session(opts).await?;

        self.waker.wake()?;
        loop {
            let frame = s.recv()?;
            match frame.performative {
                Some(Performative::Begin(_b)) => {
                    // Populate remote properties
                    return Ok(Session {
                        waker: self.waker.clone(),
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

    /// Close a connection, ending the close performative.
    pub fn close(&self, error: Option<ErrorCondition>) -> Result<()> {
        self.connection.close(error)?;
        self.waker.wake()?;
        Ok(())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _ = self.close(None);
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let _ = self.close(None);
    }
}

impl Session {
    /// Create a new sender link for a given address cross this session. The sender
    /// is returned when the other side have confirmed its existence.
    pub async fn new_sender(&self, addr: &str) -> Result<Sender> {
        let link = self.session.new_link(addr, LinkRole::Sender)?;
        trace!("Created link, waiting for attach frame");
        self.waker.wake()?;
        loop {
            let frame = self.session.recv()?;
            match frame.performative {
                Some(Performative::Attach(_a)) => {
                    // Populate remote properties
                    return Ok(Sender {
                        waker: self.waker.clone(),
                        handle: link.handle,
                        connection: self.connection.clone(),
                        link,
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

    /// Create a new receiving link for a given address cross this session. The
    /// is returned when the other side have confirmed its existence.
    pub async fn new_receiver(&self, addr: &str) -> Result<Receiver> {
        self.new_receiver_with_link_options(addr, LinkRole::Receiver)
            .await
    }

    /// Create a new receiving link for a given address cross this session. The
    /// is returned when the other side have confirmed its existence.
    pub async fn new_receiver_with_options<T: Into<ReceiverOptions>>(
        &self,
        addr: &str,
        options: T,
    ) -> Result<Receiver> {
        self.new_receiver_with_link_options(addr, options.into())
            .await
    }

    async fn new_receiver_with_link_options<T: Into<LinkOptions>>(
        &self,
        addr: &str,
        options: T,
    ) -> Result<Receiver> {
        let link = self.session.new_link_with_options(addr, options.into())?;
        trace!("Created link, waiting for attach frame");
        self.waker.wake()?;
        loop {
            let frame = self.session.recv()?;
            match frame.performative {
                Some(Performative::Attach(_a)) => {
                    // Populate remote properties
                    return Ok(Receiver {
                        waker: self.waker.clone(),
                        handle: link.handle,
                        connection: self.connection.clone(),
                        link,
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

    /// Close a session, ending the end performative.
    pub fn close(&self, error: Option<ErrorCondition>) -> Result<()> {
        self.session.close(error)?;
        self.waker.wake()?;
        Ok(())
    }
}

impl Sender {
    /// Send a message across this link. The returned disposition signals the acceptance or rejection of the message on the receiving end.
    pub async fn send(&self, mut message: Message) -> Result<Disposition> {
        let message_id = Some(Value::Ulong(
            self.next_message_id.fetch_add(1, Ordering::SeqCst),
        ));
        message.properties = message.properties.map_or_else(
            || {
                Some(MessageProperties {
                    message_id: message_id.clone(),
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
                })
            },
            |mut p| {
                p.message_id = message_id.clone();
                Some(p)
            },
        );
        let settled = false;
        let delivery = self.link.send_message(message, settled).await?;
        self.waker.wake()?;

        if !settled {
            loop {
                let frame = self.link.recv()?;
                match frame.performative {
                    Some(Performative::Disposition(ref disposition)) => {
                        let first = disposition.first;
                        let last = disposition.last.unwrap_or(first);
                        if first <= delivery.id && last >= delivery.id {
                            // TODO: Better error checking
                            return Ok(Disposition { delivery });
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
            Ok(Disposition { delivery })
        }
    }

    /// Close the sender link, sending the detach performative.
    pub fn close(&self, error: Option<ErrorCondition>) -> Result<()> {
        self.link.close(error)?;
        self.waker.wake()?;
        Ok(())
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        let _ = self.close(None);
    }
}

impl Receiver {
    /// Issue credits to the remote sender link, signalling that the receiver canaldigital
    /// accept more messages.
    pub async fn flow(&self, credit: u32) -> Result<()> {
        self.link.flow(credit).await?;
        self.waker.wake()?;
        Ok(())
    }

    /// Receive a single message across the link. The delivery is returned
    /// when a message is received.
    pub async fn receive(&self) -> Result<Delivery> {
        loop {
            let frame = self.link.recv()?;
            match frame.performative {
                Some(Performative::Transfer(ref transfer)) => {
                    let mut input = frame.payload.unwrap();
                    let message = Message::decode(&mut input)?;
                    let delivery = Arc::new(DeliveryDriver {
                        state: transfer.state.clone(),
                        tag: transfer.delivery_tag.clone().unwrap(),
                        id: transfer.delivery_id.unwrap(),
                        remotely_settled: transfer.settled.unwrap_or(false),
                        message: None,
                        settled: false,
                    });
                    return Ok(Delivery {
                        waker: self.waker.clone(),
                        settled: false,
                        link: self.link.clone(),
                        message: Some(message),
                        delivery,
                    });
                }
                _ => {
                    // TODO: Prevent reordering
                    self.link.unrecv(frame)?;
                }
            }
        }
    }

    /// Close the sender link, sending the detach performative.
    pub fn close(&self, error: Option<ErrorCondition>) -> Result<()> {
        self.link.close(error)?;
        self.waker.wake()?;
        Ok(())
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        let _ = self.close(None);
    }
}

impl Delivery {
    /// Retrieve reference to the message associated with this delivery.
    pub fn message(&self) -> &Message {
        &self.message.as_ref().unwrap()
    }

    // Take the message from the delivery to
    pub fn take_message(&mut self) -> Option<Message> {
        self.message.take()
    }

    /// Send a disposition for this delivery, indicating message settlement and delivery state.
    pub async fn disposition(&mut self, settled: bool, state: DeliveryState) -> Result<()> {
        if !self.settled {
            self.link.disposition(&self.delivery, settled, state)?;
            self.settled = settled;
            self.waker.wake()?;
        }
        Ok(())
    }
}

impl Drop for Delivery {
    fn drop(&mut self) {
        if !self.settled {
            self.settled = true;
            let _ = self
                .link
                .disposition(&self.delivery, true, DeliveryState::Accepted);
            let _ = self.waker.wake();
        }
    }
}
