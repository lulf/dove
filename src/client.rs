/*
 * Copyright 2019-2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The client module contains a simple client API for sending and receiving messages.
use crate::conn::*;
// use crate::driver::*;
use crate::error::*;
// use crate::framing::*;
// use crate::message::*;
// use std::{thread, time};

pub struct Client {}

pub struct Connection {
    // handle: ConnectionHandle,
}

/*
pub trait EventHandler {
    fn connected(&self, _: &mut ConnectionDriver) {}
    fn delivery(&self, _: &mut Link, _: &Message) {}
    fn open(&self, _: &mut ConnectionDriver) {}
    fn flow(&self, _: &mut Link) {}
    fn disposition(&self, _: &Disposition) {}
    fn close(&self, _: &mut ConnectionDriver) {}
    fn begin(&self, _: &mut Session) {}
    fn end(&self, _: &mut Session) {}
    fn attach(&self, _: &mut Link) {}
    fn detach(&self, _: &mut Link) {}
    fn error(&self, _: AmqpError) {}
}
*/

impl Client {
    pub fn new() -> Client {
        Client {}
    }

    pub async fn connect(
        self,
        _host: &str,
        _port: u16,
        _opts: ConnectionOptions,
    ) -> Result<Connection> {
        //        let handle = crate::conn::connect(host, port, opts);
        return Ok(Connection {});

        /*
        let mut driver = ConnectionDriver::new(connection);
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
        */
        Ok(())
    }
}

impl Connection {
    pub async fn new_session(&self) -> Result<Session> {
        return Ok(Session {});
    }
}

pub struct Session {}

impl Session {
    pub async fn new_sender(&self, _addr: &str) -> Result<Sender> {
        return Ok(Sender {});
    }
}

pub struct Sender {}

impl Sender {
    pub async fn send(&self, _data: &str) -> Result<Disposition> {
        return Ok(Disposition {});
    }
}

pub struct Disposition {}
