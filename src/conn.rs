/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The conn module contains basic primitives for establishing and accepting AMQP connections and performing the initial handshake. Once handshake is complete, the connection can be used to send and receive frames.

use crate::connection::ConnectionHandle;
use crate::driver::Channel;
use crate::error::*;
use crate::framing::*;
use crate::sasl::*;
use crate::transport::*;
use async_channel::Sender;
use std::sync::Arc;
use std::vec::Vec;

#[derive(Debug, Clone)]
pub struct ConnectionOptions {
    pub username: Option<String>,
    pub password: Option<String>,
    pub sasl_mechanism: Option<SaslMechanism>,
}

impl ConnectionOptions {
    #[allow(clippy::new_without_default)]
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

/*
// TODO: Listener
#[derive(Debug)]
pub struct ListenOptions {}
*/

#[derive(Debug)]
pub struct Connection<N: Network> {
    sasl: Option<Sasl>,
    state: ConnectionState,
    transport: Transport<N>,
    tx_frames: Channel<Frame>,
    header_sent: bool,
}

pub type ChannelId = u16;
pub type HandleId = u32;

#[derive(Debug)]
enum ConnectionState {
    Start,
    // TODO: Listener
    // StartWait,
    Sasl,
    Opened,
    Closed,
}

const AMQP_10_HEADER: ProtocolHeader = ProtocolHeader::AMQP(Version(1, 0, 0));
const SASL_10_HEADER: ProtocolHeader = ProtocolHeader::SASL(Version(1, 0, 0));

pub fn connect<N: Network>(
    transport: Transport<N>,
    opts: ConnectionOptions,
) -> Result<Connection<N>> {
    let mut connection = Connection::new(transport);
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

/*
pub struct Listener {
    pub listener: TcpListener,
    pub sasl_mechanisms: Option<Vec<SaslMechanism>>,
}

pub fn listen(host: &str, port: u16, _opts: ListenOptions) -> Result<Listener> {
    let addr = format!("{}:{}", host, port).parse().unwrap();
    let listener = TcpListener::bind(addr)?;
    Ok(Listener {
        listener,
        sasl_mechanisms: None,
    })
}

impl Listener {
    pub fn accept(&mut self) -> Result<Connection> {
        let (stream, _addr) = self.listener.accept()?;
        let transport: Transport = Transport::new(stream, 1024)?;

        let mut connection = Connection::new(transport);
        connection.state = ConnectionState::StartWait;
        Ok(connection)
    }
}
*/

impl<N: Network> Connection<N> {
    pub fn new(transport: Transport<N>) -> Connection<N> {
        Connection {
            transport,
            state: ConnectionState::Start,
            sasl: None,
            tx_frames: Channel::new(),
            header_sent: false,
        }
    }

    pub fn shutdown(&mut self) -> Result<()> {
        self.transport.close()
    }

    fn skip_sasl(&self) -> bool {
        self.sasl.as_ref().map(Sasl::is_done).unwrap_or(true)
    }

    // Write outgoing frames
    pub fn flush(&mut self) -> Result<()> {
        match self.state {
            ConnectionState::Opened | ConnectionState::Closed => {
                while let Ok(frame) = self.tx_frames.try_recv() {
                    debug!("TX {:?}", frame);
                    self.transport.write_frame(&frame)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub fn transport(&self) -> &Transport<N> {
        &self.transport
    }

    pub fn transport_mut(&mut self) -> &mut Transport<N> {
        &mut self.transport
    }

    pub fn process(&mut self, frames: &mut Vec<Frame>) -> Result<()> {
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
            /*
            TODO: Listener
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
                            self.state = ConnectionState::Closed;
                            self.transport.flush()?;
                            self.transport.close()?;
                        }
                    }
                }
            }
            */
            ConnectionState::Sasl => {
                if let Some(sasl) = &mut self.sasl {
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
                            sasl.perform_handshake(None, &mut self.transport)?;
                        }
                    }
                } else {
                    return Err(AmqpError::SaslConfigurationExpected);
                }
            }
            ConnectionState::Opened => {
                let frame = self.transport.read_frame()?;
                frames.push(frame);
            }
            ConnectionState::Closed => {
                return Err(AmqpError::AmqpConnectionForced);
            }
        }
        Ok(())
    }

    pub fn handle<T>(&self, param: T) -> ConnectionHandle
    where
        ConnectionHandle: From<(Sender<Frame>, (Arc<TransportInfo>, T))>,
    {
        self.tx_frames
            .handle_with((Arc::clone(self.transport.info()), param))
    }
}
