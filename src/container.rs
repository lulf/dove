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
use crate::transport;
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
use crate::options::{LinkOptions, ReceiverOptions, SenderOptions};
pub use crate::sasl::SaslMechanism;
use crate::transport::mio::MioNetwork;
pub use crate::types::{Value, ValueRef};
use std::net::{SocketAddr, ToSocketAddrs};

/// Represents an AMQP 1.0 container that can manage multiple connections.
pub struct Container {
    container: Arc<ContainerInner>,
    running: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

struct ContainerInner {
    container_id: String,
    poll: RefCell<Poll>,
    incoming: Channel<(Token, Arc<ConnectionDriver>, conn::Connection<MioNetwork>)>,
    #[allow(clippy::type_complexity)] // subjective judgement: complexity is reasonable
    connections: Mutex<HashMap<Token, (Arc<ConnectionDriver>, conn::Connection<MioNetwork>)>>,
    token_generator: AtomicU32,
    waker: Arc<Waker>,
    closed: AtomicBool,
}

/// Represents a single AMQP connection to a remote endpoint.
pub struct Connection {
    connection: Arc<ConnectionDriver>,
    waker: Arc<Waker>,

    pub container_id: String,
    pub host: SocketAddr,
    pub channel_max: u16,
    pub idle_timeout: Duration,

    pub remote_idle_timeout: Duration,
    pub remote_container_id: String,
    pub remote_channel_max: u16,
}

/// Represents an AMQP session.
pub struct Session {
    session: Arc<SessionDriver>,
}

/// Represents a sender link.
pub struct Sender {
    address: String,
    link: Arc<LinkDriver>,
    next_message_id: AtomicU64,
}

impl Sender {
    pub fn address(&self) -> &str {
        &self.address
    }
}

/// Represents a receiver link.
pub struct Receiver {
    address: String,
    link: Arc<LinkDriver>,
}

impl Receiver {
    pub fn address(&self) -> &str {
        &self.address
    }
}

/// Represents a disposition response for a sent message.
#[allow(dead_code)] // TODO this type does not seem to be useful (yet)?
pub struct Disposition {
    delivery: Arc<DeliveryDriver>,
}

/// Represent a delivery
pub struct Delivery {
    settled: bool,
    message: Option<Message>,
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
        let waker = Arc::new(Waker::new(p.registry(), Token(u32::MAX as usize))?);
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
        debug!("Starting container processing loop");
        loop {
            if !running.load(Ordering::SeqCst) {
                debug!("Stopping container processing loop");
                return;
            }
            if let Err(e) = container.process() {
                error!(
                    "{}: error while processing: {:?}",
                    container.container_id, e
                );
                break;
            }
        }
        running.store(false, Ordering::SeqCst);
        if let Err(e) = container.close() {
            error!(
                "{}: failed to properly close: {:?}",
                container.container_id, e
            );
        }
    }

    /// Connect to an AMQP endpoint and send the initial open performative.
    pub async fn connect<S: ToSocketAddrs + Send + 'static>(
        &self,
        host: S,
        opts: ConnectionOptions,
    ) -> Result<Connection> {
        self.container.connect(host, opts).await
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

        for (_id, (driver, mut connection)) in self.connections.lock().unwrap().drain() {
            let r1 = driver.close(None);
            let r2 = connection.flush();
            connection.shutdown().and(r1).and(r2)?;
        }

        self.waker.wake()?;
        trace!("{}: container is shut down", self.container_id);
        Ok(())
    }

    async fn connect<S: ToSocketAddrs + Send + 'static>(
        &self,
        host: S,
        opts: ConnectionOptions,
    ) -> Result<Connection> {
        let options = opts.clone();
        let (tx, rx) = async_channel::bounded(1);

        // mio connects in blocking mode -> new thread to not block in async context
        thread::spawn({
            move || {
                let result: Result<_> = (|| {
                    let network = transport::mio::MioNetwork::connect(&host)?;

                    if let Some(nodelay) = opts.tcp_nodelay {
                        network.set_nodelay(nodelay)?;
                    }

                    let transport =
                        transport::Transport::new(network, opts.buffer_size.unwrap_or(1024 * 1024));
                    let connection = conn::connect(transport, opts)?;
                    Ok(connection)
                })();
                let _ = tx.try_send(result);
            }
        });

        let connection = rx.recv().await??;
        let host = connection.transport().network().peer_addr();
        trace!("{}: connected to {}", self.container_id, host);

        let id = Token(self.token_generator.fetch_add(1, Ordering::SeqCst) as usize);
        debug!(
            "{}: created connection to {} with local id {:?}",
            self.container_id, host, id,
        );
        let driver = {
            let handle = connection.handle(self.waker.clone());
            let driver = Arc::new(ConnectionDriver::new(
                handle,
                options.idle_timeout.unwrap_or_default(),
            ));

            driver.open({
                let mut open = Open::new(&self.container_id);
                // open.hostname = Some(host.to_string());
                open.channel_max = Some(u16::MAX);
                open.idle_timeout = options.idle_timeout.map(|d| d.as_millis() as _);
                open
            })?;

            driver
        };

        self.incoming.send((id, driver.clone(), connection))?;
        self.waker.wake()?;

        loop {
            let frame = driver.recv().await?;
            match frame.performative {
                Some(Performative::Open(o)) => {
                    trace!("{}: received OPEN frame from {}", self.container_id, host);
                    // Populate remote properties
                    return Ok(Connection {
                        waker: self.waker.clone(),
                        connection: driver,
                        container_id: self.container_id.clone(),
                        host,
                        channel_max: u16::MAX,
                        idle_timeout: options.idle_timeout.unwrap_or_default(),

                        remote_container_id: o.container_id.clone(),
                        remote_channel_max: o.channel_max.unwrap_or(u16::MAX),
                        remote_idle_timeout: Duration::from_millis(
                            o.idle_timeout.unwrap_or(0) as u64
                        ),
                    });
                }
                Some(Performative::Close(c)) => {
                    trace!("{}: received CLOSE frame from {}", self.container_id, host);
                    return if let Some(e) = c.error {
                        Err(AmqpError::Amqp(e))
                    } else {
                        Err(AmqpError::Generic("connection closed".to_string()))
                    };
                }
                _ => {
                    // Push it back into the queue
                    // TODO: Prevent reordering
                    driver.unrecv(frame)?;
                }
            }
        }
    }

    fn process(&self) -> Result<()> {
        let mut poll = self.poll.borrow_mut();
        // Register new connections
        while let Ok((id, driver, mut connection)) = self.incoming.try_recv() {
            if let Err(e) = connection
                .transport_mut()
                .network_mut()
                .register(id, &mut poll)
            {
                let _ = driver.close(None);
                let _ = connection.shutdown();
                error!("Failed to register connection {:?}: {}", id, e);
                continue;
            } else {
                let mut connections = self.connections.lock().unwrap();
                connections.insert(id, (driver, connection));
            }
        }

        // Push connection frames on the wire
        {
            let mut connections = self.connections.lock().unwrap();
            let to_remove = connections
                .iter_mut()
                .filter_map(|(id, (driver, connection))| {
                    let result: Result<()> = (|| {
                        // Handle keepalive
                        driver.keepalive()?;
                        driver.flowcontrol()?;

                        // Flush data
                        connection.flush()?;
                        Ok(())
                    })();

                    result.err().map(|e| {
                        error!("Driver failed for container {:?}: {}", self.container_id, e);
                        let _ = driver.close(None);
                        let _ = connection.shutdown();
                        *id
                    })
                })
                .collect::<Vec<Token>>();

            for token in to_remove {
                let _ = connections.remove(&token);
            }
        }

        // Poll for new events
        let mut events = Events::with_capacity(1024);
        {
            poll.poll(&mut events, Some(Duration::from_millis(2000)))?;
        }

        let waker_token = Token(u32::MAX as usize);
        for event in &events {
            let ids = if event.token() == waker_token {
                self.connections.lock().unwrap().keys().cloned().collect()
            } else {
                vec![event.token()]
            };

            for id in ids {
                if let Err(e) = self.process_connection_by_id(id) {
                    error!("Connection with {:?} failed: {}", id, e);
                }
            }
        }
        Ok(())
    }

    fn process_connection_by_id(&self, id: Token) -> Result<()> {
        let mut m = self.connections.lock().unwrap();
        if let Some((driver, connection)) = m.get_mut(&id) {
            let close = match self.process_connection(driver, connection) {
                Err(AmqpError::Amqp(condition)) => Err(Some(condition)),
                Err(e) => {
                    error!("Error while processing a connection: {:?}", e);
                    Err(None)
                }
                _ => Ok(()),
            };

            if let Err(condition) = close {
                warn!(
                    "{}: closing connection and removing reference to {:?}: {:?}",
                    self.container_id, id, condition
                );

                if let Err(e) = driver.close(condition) {
                    error!("Closing connection {:?} failed: {}", id, e);
                }

                if let Some((_, mut connection)) = m.remove(&id) {
                    let _ = connection.shutdown();
                }

                return Err(AmqpError::IoError(std::io::Error::from(
                    std::io::ErrorKind::UnexpectedEof,
                )));
            }
        }
        Ok(())
    }

    fn process_connection(
        &self,
        driver: &ConnectionDriver,
        connection: &mut conn::Connection<MioNetwork>,
    ) -> Result<()> {
        if driver.closed() {
            return Ok(());
        }

        // Read frames until we're blocked
        let mut rx_frames = Vec::new();
        let result = loop {
            if driver.closed() {
                return Ok(());
            }
            let result = connection.process(&mut rx_frames);
            match result {
                Ok(_) => {}
                // This means that we should poll again to await further I/O action for this driver.
                Err(AmqpError::IoError(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break Ok(());
                }
                Err(ref e) => {
                    error!("Processing connection frames failed: {:?}", e);
                    break result;
                }
            }
        };

        if !rx_frames.is_empty() {
            trace!("Dispatching {:?} frames", rx_frames.len());
        }

        let dispatch_result = driver.dispatch(rx_frames);
        result.and(dispatch_result)
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
            let frame = s.recv().await?;
            match frame.performative {
                Some(Performative::Begin(_b)) => {
                    // Populate remote properties
                    return Ok(Session { session: s });
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
    pub async fn new_sender(&self, address: &str) -> Result<Sender> {
        self.new_sender_with_link_options(address, LinkRole::Sender)
            .await
    }

    /// Create a new sender link for a given address cross this session. The sender
    /// is returned when the other side have confirmed its existence.
    pub async fn new_sender_with_options(
        &self,
        address: &str,
        options: impl Into<SenderOptions>,
    ) -> Result<Sender> {
        self.new_sender_with_link_options(address, options.into())
            .await
    }

    /// Create a new sender link for a given address cross this session. The sender
    /// is returned when the other side have confirmed its existence.
    async fn new_sender_with_link_options(
        &self,
        address: &str,
        options: impl Into<LinkOptions>,
    ) -> Result<Sender> {
        let (address, link) = self.session.new_link(address, options).await?;
        Ok(Sender {
            address,
            link,
            next_message_id: AtomicU64::new(0),
        })
    }

    /// Create a new receiving link for a given address cross this session. The
    /// is returned when the other side have confirmed its existence.
    pub async fn new_receiver(&self, address: &str) -> Result<Receiver> {
        self.new_receiver_with_link_options(address, LinkRole::Receiver)
            .await
    }

    /// Create a new receiving link for a given address cross this session. The
    /// is returned when the other side have confirmed its existence.
    pub async fn new_receiver_with_options(
        &self,
        address: &str,
        options: impl Into<ReceiverOptions>,
    ) -> Result<Receiver> {
        self.new_receiver_with_link_options(address, options.into())
            .await
    }

    async fn new_receiver_with_link_options(
        &self,
        address: &str,
        options: impl Into<LinkOptions>,
    ) -> Result<Receiver> {
        let (address, link) = self.session.new_link(address, options).await?;
        Ok(Receiver { address, link })
    }

    /// Close a session, ending the end performative.
    pub fn close(&self, error: Option<ErrorCondition>) -> Result<()> {
        self.session.close(error)
    }
}

/// See also https://access.redhat.com/documentation/en-us/red_hat_amq/7.4/html/amq_clients_overview/amqp
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SendMode {
    /// Also known as "presettled" or "fire and forget".
    /// Sends the message and does not care whether it is received.
    AtMostOnce,
    /// Sends the message and ensured that its received at least once.
    AtLeastOnce,
}

impl Sender {
    /// Send a message across this link. The returned disposition signals the acceptance or rejection of the message on the receiving end.
    pub async fn send(&self, message: Message) -> Result<Disposition> {
        self.send_with_mode(message, SendMode::AtLeastOnce).await
    }

    pub async fn send_with_mode(
        &self,
        mut message: Message,
        mode: SendMode,
    ) -> Result<Disposition> {
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
        let settled = SendMode::AtMostOnce == mode;
        let delivery = self.link.send_message(message, settled).await?;
        debug!(
            "Message sent (handle {}), awaiting disposition",
            self.link.handle
        );

        if !settled {
            loop {
                let frame = self.link.recv().await?;
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
                    Some(Performative::Detach(detach)) => {
                        debug!("Link got detached: {:?}", detach);
                        let error_condition =
                            detach.error.unwrap_or_else(ErrorCondition::detach_received);
                        return Err(AmqpError::Amqp(error_condition));
                    }
                    _ => {
                        // TODO: Prevent reordering
                        warn!("({}) unreceiving: {:?}", self.link.handle, frame);
                        self.link.unrecv(frame)?;
                    }
                }
            }
        } else {
            Ok(Disposition { delivery })
        }
    }

    /// Retrieve credits available on this link. 0 means a send will fail with NotEnoughCreditsToSend.
    pub fn credits(&self) -> u32 {
        self.link.credits()
    }

    /// Close the sender link, sending the detach performative.
    pub fn close(&self, error: Option<ErrorCondition>) -> Result<()> {
        self.link.close(error)
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
    pub fn flow(&self, credit: u32) -> Result<()> {
        self.link.flow(credit)
    }

    /// Receive a single message across the link. The delivery is returned
    /// when a message is received.
    pub async fn receive(&self) -> Result<Delivery> {
        loop {
            let frame = self.link.recv().await?;
            match frame.performative {
                Some(Performative::Transfer(transfer)) => {
                    if let Some(mut input) = frame.payload {
                        let message = Message::decode(&mut input)?;
                        let delivery = Arc::new(DeliveryDriver {
                            state: transfer.state,
                            tag: transfer
                                .delivery_tag
                                .ok_or(AmqpError::TransferFrameIsMissingDeliveryTag)?,
                            id: transfer
                                .delivery_id
                                .ok_or(AmqpError::TransferFrameIsMissingDeliveryTag)?,
                            remotely_settled: transfer.settled.unwrap_or(false),
                            message: None,
                            settled: false,
                        });
                        return Ok(Delivery {
                            settled: false,
                            link: self.link.clone(),
                            message: Some(message),
                            delivery,
                        });
                    } else {
                        return Err(AmqpError::TransferFrameIsMissingPayload);
                    }
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
        self.link.close(error)
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
        self.message.as_ref().unwrap()
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
        }
        Ok(())
    }
}

impl Drop for Delivery {
    fn drop(&mut self) {
        if !self.settled {
            self.settled = true;
            if let Err(e) = self
                .link
                .disposition(&self.delivery, true, DeliveryState::Accepted)
            {
                error!(
                    "Disposition failed for delivery with id {}: {:?}",
                    self.delivery.id, e
                );
            }
        }
    }
}
