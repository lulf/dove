/*
 * Copyright 2019-2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The client module contains a simple client API for sending and receiving messages.
use crate::conn::*;
use crate::driver::*;
use crate::error::*;
use mio::event;
use mio::{Events, Interest, Poll, Registry, Token};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
// use crate::framing::*;
// use crate::message::*;
use std::marker::{Send, Sync};
use std::sync::mpsc;
use std::time::Duration;

pub struct Client {
    poll: Mutex<Poll>,
    incoming: Channel<Token>,
    connections: Mutex<HashMap<Token, Arc<Connection>>>,
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

pub struct Connection {
    id: Token,
    driver: Arc<Mutex<ConnectionDriver>>,
    sessions: Mutex<HashMap<ChannelId, Arc<Session>>>,
    incoming: Channel<ChannelId>,
    opened: Channel<()>,
}

pub struct Session {
    opened: Channel<()>,
    channel: ChannelId,
}

trait EventHandler {
    fn on_event(&mut self, driver: &mut ConnectionDriver, event: &Event);
}

impl Client {
    pub fn new() -> Result<Client> {
        let p = Poll::new()?;
        Ok(Client {
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
    ) -> Result<Arc<Connection>> {
        let handle = crate::conn::connect(host, port, opts)?;
        println!("Connected!");
        let mut driver = ConnectionDriver::new(1, handle);
        let id = driver.token();

        println!("Done registerting connection driver");
        driver.open();

        let conn = Arc::new(Connection {
            id: id,
            driver: Arc::new(Mutex::new(driver)),
            opened: Channel::new(),
            incoming: Channel::new(),
            sessions: Mutex::new(HashMap::new()),
        });

        {
            let mut m = self.connections.lock().unwrap();
            m.insert(id, conn.clone());
        }

        self.incoming.send(id)?;

        println!("Waiting until opened...");
        let _ = conn.opened.recv()?;
        return Ok(conn);
    }

    pub fn process(&self) -> Result<()> {
        let mut events = Events::with_capacity(1024);

        let mut tokens = HashSet::new();

        // Register new connections
        loop {
            let mut result = self.incoming.try_recv();
            match result {
                Err(_) => break,
                Ok(id) => {
                    println!("Got new incoming!");
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

        // Check connections for events.
        for (id, mut connection) in self.connections.lock().unwrap().iter_mut() {
            loop {
                let mut result = connection.incoming.try_recv();
                match result {
                    Err(_) => break,
                    Ok(_) => {
                        tokens.insert(*id);
                    }
                }
            }
        }

        // Process connection that have had incoming events
        for id in tokens.iter() {
            let connection = {
                let m = self.connections.lock().unwrap();
                m.get(&id).map(|c| c.clone())
            };
            match connection {
                Some(c) => c.process()?,
                _ => {}
            }
        }

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

impl Connection {
    fn process(&self) -> Result<()> {
        // Do work until we get blocked
        let mut event_buffer = EventBuffer::new();
        loop {
            let mut driver = self.driver.lock().unwrap();
            println!("Processing connection");
            match driver.do_work(&mut event_buffer) {
                Ok(_) => {
                    for event in event_buffer.drain(..) {
                        match event {
                            Event::RemoteOpen(_, _) => {
                                println!("GOT REMOTE OPEN");
                                self.opened.send(())?;
                            }
                            Event::RemoteBegin(_, chan, _) => {
                                println!("GOT REMOTE BEGIN");
                                let session = {
                                    let mut m = self.sessions.lock().unwrap();
                                    m.get_mut(&chan).unwrap().clone()
                                };
                                session.opened.send(())?;
                            }
                            _ => {
                                println!("Got unhandled event: {:?}", event);
                            }
                        }
                    }
                }
                // This means that we should poll again to await further I/O action for this driver.
                Err(AmqpError::IoError(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    return Ok(());
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    pub async fn new_session(&self) -> Result<Arc<Session>> {
        let id = {
            let mut driver = self.driver.lock().unwrap();
            let mut s = driver.create_session();
            s.open();
            self.sessions.lock().unwrap().insert(
                s.local_channel,
                Arc::new(Session {
                    channel: s.local_channel,
                    opened: Channel::new(),
                }),
            );
            s.local_channel
        };

        self.incoming.send(id)?;

        // Wait until it has been opened
        let mut s = {
            let mut m = self.sessions.lock().unwrap();
            m.get_mut(&id).unwrap().clone()
        };
        println!("Awaiting sessions to be opened");
        let _ = s.opened.recv()?;
        println!("SESSION OPENED");
        Ok(s)
    }
}

impl EventHandler for Connection {
    fn on_event(&mut self, driver: &mut ConnectionDriver, event: &Event) {}
}

impl Session {
    pub async fn new_sender(&self, _addr: &str) -> Result<Sender> {
        return Ok(Sender {});
    }
    pub async fn new_receiver(&self, _addr: &str) -> Result<Receiver> {
        return Ok(Receiver {});
    }
}

pub struct Sender {}

impl Sender {
    pub async fn send(&self, _data: &str) -> Result<Disposition> {
        return Ok(Disposition {});
    }
}

pub struct Disposition {}

pub struct Receiver {}

impl Receiver {
    pub async fn receive(&self) -> Result<Delivery> {
        return Ok(Delivery {});
    }
}

pub struct Delivery {}

impl Delivery {
    pub fn message(&self) -> Result<String> {
        return Ok(String::new());
    }
}
