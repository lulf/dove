/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The conn module contains basic primitives for establishing and accepting AMQP connections and performing the initial handshake. Once handshake is complete, the connection can be used to send and receive frames.

use log::trace;
use mio::event::Source;
use mio::net::TcpListener;
use mio::net::TcpStream;
use mio::{Interest, Registry, Token};
use std::net::ToSocketAddrs;
use std::time::Duration;
use std::time::Instant;
use std::vec::Vec;

use crate::error::*;
use crate::framing::*;
use crate::sasl::*;
use crate::transport::*;

#[derive(Debug)]
pub struct ConnectionOptions {
    pub username: Option<String>,
    pub password: Option<String>,
    pub sasl_mechanism: Option<SaslMechanism>,
}

impl ConnectionOptions {
    pub fn new() -> ConnectionOptions {
        ConnectionOptions {
            username: None,
            password: None,
            sasl_mechanism: None,
        }
    }

    pub fn sasl_mechanism(mut self, mechanism: SaslMechanism) -> Self {
        self.sasl_mechanism = Some(mechanism);
        self
    }

    pub fn username(mut self, username: &str) -> Self {
        self.username = Some(username.to_string());
        self
    }

    pub fn password(mut self, password: &str) -> Self {
        self.password = Some(password.to_string());
        self
    }
}

#[derive(Debug)]
pub struct ListenOptions {}

#[derive(Debug)]
pub struct Connection {
    pub hostname: String,
    sasl: Option<Sasl>,
    state: ConnectionState,
    transport: Transport,
    tx_frames: Vec<Frame>,
    header_sent: bool,
}

pub type ChannelId = u16;
pub type HandleId = u32;

#[derive(Debug)]
enum ConnectionState {
    Start,
    StartWait,
    Sasl,
    Opened,
    Closed,
}

const AMQP_10_HEADER: ProtocolHeader = ProtocolHeader::AMQP(Version(1, 0, 0));
const SASL_10_HEADER: ProtocolHeader = ProtocolHeader::SASL(Version(1, 0, 0));

pub fn connect(host: &str, port: u16, opts: ConnectionOptions) -> Result<Connection> {
    let mut addrs = format!("{}:{}", host, port).to_socket_addrs().unwrap();
    let stream = TcpStream::connect(addrs.next().unwrap())?;
    let transport: Transport = Transport::new(stream, 1024)?;

    let mut connection = Connection::new(host, transport);
    if opts.username.is_some() || opts.password.is_some() || opts.sasl_mechanism.is_some() {
        connection.sasl = Some(Sasl {
            role: SaslRole::Client(SaslClient {
                mechanism: opts.sasl_mechanism.unwrap_or(SaslMechanism::Plain),
                username: opts.username,
                password: opts.password,
            }),
            state: SaslState::InProgress,
        });
    }
    connection.state = ConnectionState::Start;

    Ok(connection)
}

pub struct Listener {
    pub listener: TcpListener,
    pub sasl_mechanisms: Option<Vec<SaslMechanism>>,
}

pub fn listen(host: &str, port: u16, _opts: ListenOptions) -> Result<Listener> {
    let addr = format!("{}:{}", host, port).parse().unwrap();
    let listener = TcpListener::bind(addr)?;
    Ok(Listener {
        listener: listener,
        sasl_mechanisms: None,
    })
}

impl Listener {
    pub fn accept(&mut self) -> Result<Connection> {
        let (stream, addr) = self.listener.accept()?;
        let transport: Transport = Transport::new(stream, 1024)?;

        let mut connection = Connection::new(addr.ip().to_string().as_str(), transport);
        connection.state = ConnectionState::StartWait;
        Ok(connection)
    }
}

impl Connection {
    pub fn new(hostname: &str, transport: Transport) -> Connection {
        Connection {
            hostname: hostname.to_string(),
            transport: transport,
            state: ConnectionState::Start,
            sasl: None,
            tx_frames: Vec::new(),
            header_sent: false,
        }
    }

    pub fn open(self: &mut Self, open: Open) -> Result<()> {
        self.tx_frames.push(Frame::AMQP(AmqpFrame {
            channel: 0,
            performative: Some(Performative::Open(open)),
            payload: None,
        }));
        Ok(())
    }

    pub fn begin(self: &mut Self, channel: ChannelId, begin: Begin) -> Result<()> {
        self.tx_frames.push(Frame::AMQP(AmqpFrame {
            channel: channel as u16,
            performative: Some(Performative::Begin(begin)),
            payload: None,
        }));
        Ok(())
    }

    pub fn attach(self: &mut Self, channel: ChannelId, attach: Attach) -> Result<()> {
        self.tx_frames.push(Frame::AMQP(AmqpFrame {
            channel: channel as u16,
            performative: Some(Performative::Attach(attach)),
            payload: None,
        }));
        Ok(())
    }

    pub fn flow(self: &mut Self, channel: ChannelId, flow: Flow) -> Result<()> {
        self.tx_frames.push(Frame::AMQP(AmqpFrame {
            channel: channel as u16,
            performative: Some(Performative::Flow(flow)),
            payload: None,
        }));
        Ok(())
    }

    pub fn transfer(
        self: &mut Self,
        channel: ChannelId,
        transfer: Transfer,
        payload: Option<Vec<u8>>,
    ) -> Result<()> {
        self.tx_frames.push(Frame::AMQP(AmqpFrame {
            channel: channel as u16,
            performative: Some(Performative::Transfer(transfer)),
            payload: payload,
        }));
        Ok(())
    }

    pub fn disposition(
        self: &mut Self,
        channel: ChannelId,
        disposition: Disposition,
    ) -> Result<()> {
        self.tx_frames.push(Frame::AMQP(AmqpFrame {
            channel: channel as u16,
            performative: Some(Performative::Disposition(disposition)),
            payload: None,
        }));
        Ok(())
    }

    pub fn keepalive(
        self: &mut Self,
        remote_idle_timeout: Duration,
        now: Instant,
    ) -> Result<Instant> {
        if remote_idle_timeout.as_millis() > 0 {
            /*
            trace!(
                "Remote idle timeout millis: {:?}. Last sent: {:?}",
                self.remote_idle_timeout.as_millis(),
                now - self.transport.last_sent()
            );
            */

            if now - self.transport.last_sent() >= remote_idle_timeout {
                self.tx_frames.push(Frame::AMQP(AmqpFrame {
                    channel: 0,
                    performative: None,
                    payload: None,
                }));
            }
        }
        Ok(self.transport.last_received())
    }

    pub fn detach(self: &mut Self, channel: ChannelId, detach: Detach) -> Result<()> {
        self.tx_frames.push(Frame::AMQP(AmqpFrame {
            channel: channel as u16,
            performative: Some(Performative::Detach(detach)),
            payload: None,
        }));
        Ok(())
    }

    pub fn end(self: &mut Self, channel: ChannelId, end: End) -> Result<()> {
        self.tx_frames.push(Frame::AMQP(AmqpFrame {
            channel: channel as u16,
            performative: Some(Performative::End(end)),
            payload: None,
        }));
        Ok(())
    }

    pub fn close(self: &mut Self, close: Close) -> Result<()> {
        self.tx_frames.push(Frame::AMQP(AmqpFrame {
            channel: 0,
            performative: Some(Performative::Close(close)),
            payload: None,
        }));
        Ok(())
    }

    pub fn shutdown(self: &mut Self) -> Result<()> {
        self.transport.close()
    }

    fn skip_sasl(self: &Self) -> bool {
        self.sasl.is_none() || self.sasl.as_ref().unwrap().is_done()
    }

    // Write outgoing frames
    pub fn flush(self: &mut Self) -> Result<()> {
        match self.state {
            ConnectionState::Opened | ConnectionState::Closed => {
                for frame in self.tx_frames.drain(..) {
                    trace!("TX {:?}", frame);
                    self.transport.write_frame(&frame)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub fn process(self: &mut Self, frames: &mut Vec<Frame>) -> Result<()> {
        match self.state {
            ConnectionState::Start => {
                if !self.header_sent {
                    if self.skip_sasl() {
                        self.transport.write_protocol_header(&AMQP_10_HEADER)?;
                    } else {
                        self.transport.write_protocol_header(&SASL_10_HEADER)?;
                    }
                    self.header_sent = true;
                }
                self.transport.flush()?;
                let header = self.transport.read_protocol_header()?;
                if let Some(header) = header {
                    match header {
                        SASL_10_HEADER if self.sasl.is_some() => {
                            self.state = ConnectionState::Sasl;
                        }
                        AMQP_10_HEADER if self.skip_sasl() => {
                            self.state = ConnectionState::Opened;
                        }
                        _ => {
                            self.transport.close()?;
                            self.state = ConnectionState::Closed;
                        }
                    }
                }
            }
            ConnectionState::StartWait => {
                let header = self.transport.read_protocol_header()?;
                if let Some(header) = header {
                    match header {
                        SASL_10_HEADER if self.sasl.is_some() => {
                            self.transport.write_protocol_header(&SASL_10_HEADER)?;
                            self.state = ConnectionState::Sasl;
                            self.transport.flush()?;
                        }
                        AMQP_10_HEADER if self.skip_sasl() => {
                            self.transport.write_protocol_header(&AMQP_10_HEADER)?;
                            self.state = ConnectionState::Opened;
                            self.transport.flush()?;
                        }
                        _ => {
                            self.transport.write_protocol_header(&AMQP_10_HEADER)?;
                            self.transport.flush()?;
                            self.transport.close()?;
                        }
                    }
                }
            }
            ConnectionState::Sasl => {
                let sasl = self.sasl.as_mut().unwrap();
                match sasl.state {
                    SaslState::Success => {
                        self.header_sent = false;
                        self.state = ConnectionState::Start;
                    }
                    SaslState::Failed => {
                        self.transport.close()?;
                        self.state = ConnectionState::Closed;
                    }
                    SaslState::InProgress => {
                        sasl.perform_handshake(&self.hostname, &mut self.transport)?;
                    }
                }
            }
            ConnectionState::Opened => {
                frames.push(self.transport.read_frame()?);
            }
            ConnectionState::Closed => {
                return Err(AmqpError::amqp_error(
                    condition::connection::CONNECTION_FORCED,
                    None,
                ))
            }
        }
        Ok(())
    }
}

impl Source for Connection {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> std::io::Result<()> {
        self.transport.register(registry, token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> std::io::Result<()> {
        self.transport.reregister(registry, token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> std::io::Result<()> {
        self.transport.deregister(registry)
    }
}
