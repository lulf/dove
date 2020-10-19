/*
 * Copyright 2019-2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The client module contains a simple client API for sending and receiving messages.
use crate::conn::*;
use crate::driver::*;
use crate::error::*;
use crate::framing::*;
use crate::message::*;
use std::{thread, time};

pub struct Client {
    handler: Box<dyn EventHandler + Send>,
}

pub trait EventHandler {
    fn connected(&self, _: &mut ConnectionHandle) {}
    fn delivery(&self, _: &mut Link, _: &Message) {}
    fn open(&self, _: &mut ConnectionHandle) {}
    fn flow(&self, _: &mut Link) {}
    fn disposition(&self, _: &Disposition) {}
    fn close(&self, _: &mut ConnectionHandle) {}
    fn begin(&self, _: &mut Session) {}
    fn end(&self, _: &mut Session) {}
    fn attach(&self, _: &mut Link) {}
    fn detach(&self, _: &mut Link) {}
    fn error(&self, _: AmqpError) {}
}

impl Client {
    pub fn new(handler: Box<dyn EventHandler + Send>) -> Client {
        Client { handler: handler }
    }

    pub fn connect(self, host: &str, port: u16, opts: ConnectionOptions) -> Result<()> {
        let connection = crate::conn::connect(host, port, opts)?;

        let mut driver = ConnectionDriver::new()?;
        driver.register(1, connection)?;
        thread::spawn(move || loop {
            let mut event_buffer = EventBuffer::new();
            match driver.poll(&mut event_buffer, None) {
                Err(e) => {
                    self.handler.error(e);
                }
                Ok(_) => {
                    for event in event_buffer.drain(..) {
                        match event {
                            Event::ConnectionInit(cid) => {
                                self.handler.connected(driver.connection(cid).unwrap());
                            }
                            Event::RemoteOpen(cid, _) => {
                                self.handler.open(driver.connection(cid).unwrap())
                            }
                            Event::RemoteBegin(cid, chan, _) => {
                                let conn = driver.connection(cid).unwrap();
                                let session = conn.get_session(chan).unwrap();
                                self.handler.begin(session);
                            }
                            Event::RemoteAttach(cid, chan, handle, _) => {
                                let conn = driver.connection(cid).unwrap();
                                let session = conn.get_session(chan).unwrap();
                                let link = session.get_link(handle).unwrap();
                                self.handler.attach(link);
                            }
                            Event::RemoteDetach(cid, chan, handle, _) => {
                                let conn = driver.connection(cid).unwrap();
                                let session = conn.get_session(chan).unwrap();
                                let link = session.get_link(handle).unwrap();
                                self.handler.detach(link);
                            }
                            Event::RemoteEnd(cid, chan, _) => {
                                let conn = driver.connection(cid).unwrap();
                                let session = conn.get_session(chan).unwrap();
                                self.handler.end(session);
                            }
                            Event::RemoteClose(cid, _) => {
                                let conn = driver.connection(cid).unwrap();
                                self.handler.close(conn);
                            }
                            Event::Flow(cid, chan, handle, _) => {
                                let conn = driver.connection(cid).unwrap();
                                let session = conn.get_session(chan).unwrap();
                                let link = session.get_link(handle).unwrap();
                                self.handler.flow(link);
                            }
                            Event::Delivery(cid, chan, handle, delivery) => {
                                let conn = driver.connection(cid).unwrap();
                                let session = conn.get_session(chan).unwrap();
                                let link = session.get_link(handle).unwrap();
                                self.handler.delivery(link, &delivery.message);
                            }
                            Event::Disposition(_, _, disposition) => {
                                self.handler.disposition(&disposition);
                            }
                            _ => {}
                        }
                    }
                }
            }
            thread::sleep(time::Duration::from_millis(10));
        });
        Ok(())
    }
}
