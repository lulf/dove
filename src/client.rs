/*
 * Copyright 2019-2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The client module contains a simple client API for sending and receiving messages.
use crate::conn;
use crate::conn::{ChannelId, ConnectionOptions};
use crate::error::*;
use crate::framing::{AmqpFrame, Close, Frame, Open, Performative};
use log::trace;
// use mio::event;
use mio::{Events, Interest, Poll, Token};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
// use crate::message::*;
use std::sync::mpsc;
use std::time::{Duration, Instant};

pub struct Client {
    container_id: String,
    poll: Mutex<Poll>,
    incoming: Channel<Token>,
    connections: Mutex<HashMap<Token, Arc<ConnectionInner>>>,
}

pub struct Channel<T> {
    tx: Mutex<mpsc::Sender<T>>,
    rx: Mutex<mpsc::Receiver<T>>,
}

pub type DeliveryTag = Vec<u8>;

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

pub struct Connection {
    inner: Arc<ConnectionInner>,

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
    driver: Mutex<conn::Connection>,
    sessions: Mutex<HashMap<ChannelId, Arc<Session>>>,

    // Frames received on this connection
    rx: Channel<Performative>,
    remote_channel_map: Mutex<HashMap<ChannelId, ChannelId>>,
    remote_idle_timeout: Duration,
}

pub struct Session {
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
links: Mutex<HashMap<HandleId, Arc<Link>>>,
*/}

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
            driver: Mutex::new(driver),
            rx: Channel::new(),
            sessions: Mutex::new(HashMap::new()),
            remote_channel_map: Mutex::new(HashMap::new()),
            idle_timeout: Duration::from_secs(5),
            remote_idle_timeout: Duration::from_secs(0),
            channel_max: std::u16::MAX,
        });

        // TODO: Increment
        let id = Token(1);
        {
            let mut m = self.connections.lock().unwrap();
            m.insert(id, conn.clone());
        }
        self.incoming.send(id)?;

        println!("Waiting until opened...");
        loop {
            let frame = conn.rx.recv()?;
            match frame {
                Performative::Open(o) => {
                    // Populate remote properties
                    return Ok(Connection {
                        inner: conn.clone(),
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
        let mut driver = self.driver.lock().unwrap();

        // Read frames until we're blocked
        let mut rx_frames = Vec::new();
        loop {
            let result = driver.process(&mut rx_frames);
            match result {
                Ok(_) => {}
                // This means that we should poll again to await further I/O action for this driver.
                Err(AmqpError::IoError(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        println!("Got {:?} frames", rx_frames.len());

        // Process received frames.
        for frame in rx_frames.drain(..) {
            if let Frame::AMQP(AmqpFrame {
                channel: _,
                performative,
                payload: _,
            }) = frame
            {
                println!("Got AMQP frame: {:?}", performative);
                self.rx.send(performative.unwrap())?;
            }
        }
        Ok(())
    }
}

impl Connection {
    fn allocate_session(
        self: &Self,
        _remote_channel_id: Option<ChannelId>,
    ) -> Option<Arc<Session>> {
        let mut m = self.inner.sessions.lock().unwrap();
        for i in 0..self.channel_max {
            let chan = i as ChannelId;
            if !m.contains_key(&chan) {
                let session = Arc::new(Session {
       //             driver: self.inner.clone(),
                    /*
                    remote_channel: remote_channel_id,
                    local_channel: chan,
                    handle_max: std::u32::MAX,
                    delivery_to_handle: HashMap::new(),
                    next_outgoing_id: 0,
                    links: Mutex::new(HashMap::new()),

                    opts: None,
                    incoming: Channel::new(),
                    opened: Channel::new(),
                    */
                });
                m.insert(chan, session.clone());
                return Some(session);
            }
        }
        None
    }

    pub async fn new_session(&self, _opts: Option<SessionOpts>) -> Result<Arc<Session>> {
        Ok(self.allocate_session(None).unwrap())

        // self.sessions.insert(chan, s);
        // channel_id.map(|c| self.remote_channel_map.insert(c, chan));
        //self.sessions.get_mut(&chan).unwrap()
        /*
        let id = {
            let mut driver = self.driver.lock().unwrap();
            let s = driver.create_session();
            s.open();
            self.sessions.lock().unwrap().insert(
                s.local_channel,
                Arc::new(Session {
                    driver: self.driver.clone(),
                    opts: opts.unwrap_or(SessionOpts {
                        max_frame_size: std::u32::MAX,
                    }),
                    incoming: Channel::new(),
                    links: Mutex::new(HashMap::new()),
                    opened: Channel::new(),
                }),
            );
            s.local_channel
        };
            */

        /*
        self.incoming.send(id)?;

        // Wait until it has been opened
        let s = {
            let mut m = self.sessions.lock().unwrap();
            m.get_mut(&id).unwrap().clone()
        };
        println!("Awaiting sessions to be opened");
        let _ = s.opened.recv()?;
        println!("SESSION OPENED");
        Ok(s)*/
    }
}

impl Session {
    pub async fn new_sender(&self, addr: &str) -> Result<Arc<Link>> {
        self.new_link(addr, true).await
    }

    async fn new_link(&self, _addr: &str, _sender: bool) -> Result<Arc<Link>> {
        Ok(Arc::new(Link {}))
        /*
        let address = Some(addr.to_string());
            let link = Link {
                name: addr.to_string(),
                handle: 1,
                next_message_id: 0,
                role: LinkRole::Sender,
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
                deliveries: Vec::new(),
                dispositions: Vec::new(),
                unsettled: HashMap::new(),
                flow: Vec::new(),
            };
        let id = {
            let mut driver = self.driver.lock().unwrap();

        );

        self.incoming.send(id)?;

        // Wait until it has been opened
        let s = {
            let mut m = self.links.lock().unwrap();
            m.get_mut(&id).unwrap().clone()
        };
        println!("Awaiting link to be opened");
        let _ = s.opened.recv()?;
        println!("LINK OPENED");
        Ok(s)
        */
    }

    pub async fn new_receiver(&self, addr: &str) -> Result<Arc<Link>> {
        self.new_link(addr, false).await
    }
}

pub struct Link {
    /*
driver: Arc<Mutex<conn::Connection>>,
channel: ChannelId,
handle: HandleId,
opened: Channel<()>,
deliveries: Mutex<HashMap<DeliveryTag, Arc<Channel<Disposition>>>>,
*/}

impl Link {
    pub async fn send(&self, _data: &str) -> Result<Disposition> {
        Ok(Disposition {})
        /*
        let mut driver = self.driver.lock().unwrap();
        let session = driver.get_session(self.channel).unwrap();
        let link = session.get_link(self.handle).unwrap();
        let c = Arc::new(Channel::new());
        {
            let mut m = self.deliveries.lock().unwrap();
            let delivery = link.send(data)?;
            m.insert(delivery.tag, c.clone());
        }

        let disposition = c.recv()?;

        return Ok(disposition);
        */
    }

    pub async fn receive(&self) -> Result<Delivery> {
        return Ok(Delivery {});
    }
}

pub struct Disposition {
    /*    pub remote_settled: bool,
pub state: Option<DeliveryState>,*/}

pub struct Delivery {}

impl Delivery {
    pub fn message(&self) -> Result<String> {
        return Ok(String::new());
    }
}
