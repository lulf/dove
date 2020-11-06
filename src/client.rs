/*
 * Copyright 2019-2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The client module contains a simple client API for sending and receiving messages.
use crate::conn::*;
use crate::driver::*;
use crate::error::*;
use mio::{Events, Interest, Poll, Registry, Token};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
// use crate::framing::*;
// use crate::message::*;
use std::sync::mpsc;
use std::time::Duration;

pub struct Client {
    poll: Mutex<Poll>,
    connections: Mutex<HashMap<Token, Arc<Connection>>>,
}

pub struct Channel {
    tx: Mutex<mpsc::Sender<()>>,
    rx: Mutex<mpsc::Receiver<()>>,
}

impl Channel {
    fn new() -> Channel {
        let (tx, rx) = mpsc::channel();
        return Channel {
            tx: Mutex::new(tx),
            rx: Mutex::new(rx),
        };
    }

    fn notify(&self) -> Result<()> {
        self.tx.lock().unwrap().send(())?;
        Ok(())
    }

    fn wait(&self) -> Result<()> {
        self.rx.lock().unwrap().recv()?;
        Ok(())
    }
}

pub struct Connection {
    driver: Arc<Mutex<ConnectionDriver>>,
    sessions: Mutex<HashMap<ChannelId, Arc<Session>>>,
    opened: Channel,
}

pub struct Session {
    opened: Channel,
    channel: ChannelId,
}

trait EventHandler {
    fn on_event(&mut self, driver: &mut ConnectionDriver, event: &Event);
}

impl Client {
    pub fn new() -> Result<Client> {
        Ok(Client {
            poll: Mutex::new(Poll::new()?),
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
        driver.open();

        let c = Connection {
            driver: Arc::new(Mutex::new(driver)),
            opened: Channel::new(),
            sessions: Mutex::new(HashMap::new()),
        };

        let conn = {
            let mut m = self.connections.lock().unwrap();
            m.insert(id, Arc::new(c));
            m.get(&id).unwrap().clone()
        };

        println!("Locking driver for registration");
        {
            self.poll.lock().unwrap().registry().register(
                &mut driver,
                id,
                Interest::READABLE | Interest::WRITABLE,
            )?;
        }
        println!("Waiting until opened...");
        conn.opened.wait()?;
        return Ok(conn);
    }

    pub fn process(&self) -> Result<()> {
        let mut events = Events::with_capacity(1024);
        {
            println!("Calling poll");
            self.poll
                .lock()
                .unwrap()
                .poll(&mut events, Some(Duration::from_secs(5)))?;
        }
        for event in &events {
            let id = event.token();
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
            match driver.do_work(&mut event_buffer) {
                Ok(_) => {
                    for event in event_buffer.drain(..) {
                        match event {
                            Event::RemoteOpen(_, _) => {
                                println!("GOT REMOTE OPEN");
                                self.opened.notify()?;
                            }
                            Event::RemoteBegin(_, chan, _) => {
                                let session = {
                                    let mut m = self.sessions.lock().unwrap();
                                    m.get_mut(&chan).unwrap().clone()
                                };
                                session.opened.notify()?;
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

        // Wait until it has been opened
        let mut s = {
            let mut m = self.sessions.lock().unwrap();
            m.get_mut(&id).unwrap().clone()
        };
        s.opened.wait()?;
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
