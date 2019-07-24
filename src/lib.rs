/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

mod error;
mod framing;
mod types;

use std::convert::From;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::vec::Vec;

use error::Result;
use error::*;

pub struct ConnectionOptions {
    host: &'static str,
    port: u16,
}

pub struct Container {
    id: String,
}

enum ConnectionState {
    Start,
    HdrRcvd,
    HdrSent,
    HdrExch,
    //OpenPipe,
    //OcPipe,
    OpenRcvd,
    OpenSent,
    //ClosePipe,
    Opened,
    CloseRcvd,
    CloseSent,
    //Discarding,
    End,
}

pub struct Session {}

pub struct Link {}

pub struct Sender {}

pub struct Receiver {}

const AMQP_10_VERSION: [u8; 8] = [65, 77, 81, 80, 0, 1, 0, 0];

pub struct Connection {
    opts: ConnectionOptions,
    pub container_id: String,
    transport: Transport,
    sessions: Vec<Session>,
    state: ConnectionState,
    pending_events: Vec<Event>,
}

pub struct Transport {
    stream: TcpStream,
}

impl Container {
    pub fn new(id: &'static str) -> Container {
        Container {
            id: String::from(id),
        }
    }

    pub fn connect(&self, opts: ConnectionOptions) -> Result<Connection> {
        let stream = TcpStream::connect(format!("{}:{}", opts.host, opts.port))?;
        stream.set_nonblocking(true)?;
        // TODO: SASL support

        let transport: Transport = Transport { stream: stream };

        let connection = Connection {
            sessions: Vec::new(),
            container_id: self.id.clone(),
            transport: transport,
            pending_events: Vec::new(),
            opts: opts,
            state: ConnectionState::Start,
        };

        Ok(connection)
    }
}

/*
pub fn listen(&self, opts: ListenOptions) -> io::Result<()> {
    return Ok(());
}
*/

struct Context<'a> {
    connection: Option<&'a Connection>,
}

#[derive(Debug)]
pub enum Event {
    ConnectionInit,
    RemoteOpen(framing::Open),
    LocalOpen(framing::Open),
    UnknownFrame(framing::Frame),
}

impl Connection {
    // Wait for next state event
    pub fn next_event(self: &mut Self) -> Result<Event> {
        if self.pending_events.len() > 0 {
            Ok(self.pending_events.remove(0))
        } else {
            loop {
                println!("Checking state...");
                match self.state {
                    ConnectionState::Start => {
                        self.transport.stream.write(&AMQP_10_VERSION)?;
                        self.state = ConnectionState::HdrSent;
                        println!("Moved to HdrSent!");
                        continue;
                    }
                    ConnectionState::HdrSent => {
                        let mut remote_version: [u8; 8] = [0; 8];
                        println!("Reading remote version...");
                        self.transport.stream.read_exact(&mut remote_version)?;
                        println!("Done!");
                        println!("Received remote version buf: {:?}", remote_version);
                        self.state = ConnectionState::HdrExch;
                        return Ok(Event::ConnectionInit);
                    }
                    ConnectionState::HdrRcvd => {
                        self.transport.stream.write(&AMQP_10_VERSION)?;
                        self.state = ConnectionState::HdrExch;
                        return Ok(Event::ConnectionInit);
                    }
                    ConnectionState::HdrExch => {
                        return self.handle_open(ConnectionState::OpenRcvd);
                    }
                    ConnectionState::OpenSent => {
                        return self.handle_open(ConnectionState::Opened);
                    }
                    ConnectionState::Opened => {
                        let frame = framing::decode_frame(&mut self.transport.stream)?;
                        return Ok(Event::UnknownFrame(frame));
                    }
                    _ => return Err(AmqpError::NotImplemented),
                }
            }
        }
    }

    fn handle_open(self: &mut Self, next_state: ConnectionState) -> Result<Event> {
        // Read incoming data if we have some
        let frame = framing::decode_frame(&mut self.transport.stream)?;
        match frame {
            framing::Frame::AMQP {
                channel,
                performative,
                payload,
            } => {
                // Handle AMQP
                let performative =
                    performative.expect("Missing required performative for AMQP frame");
                match performative {
                    framing::Performative::Open(open) => {
                        self.state = next_state;
                        return Ok(Event::RemoteOpen(open));
                    }
                }
            }
            _ => {
                return Err(AmqpError::InternalError(String::from("Framing error")));
            }
        }
    }

    pub fn open(self: &mut Self) -> Result<()> {
        match self.state {
            ConnectionState::HdrExch => self.do_open(ConnectionState::OpenSent),
            ConnectionState::OpenRcvd => self.do_open(ConnectionState::Opened),
            _ => Err(AmqpError::InternalError(String::from("Already opened"))),
        }
    }

    fn do_open(self: &mut Self, next_state: ConnectionState) -> Result<()> {
        let frame = framing::Frame::AMQP {
            channel: 0,
            performative: Some(framing::Performative::Open(framing::Open {
                hostname: String::from(self.opts.host),
                ..Default::default()
            })),
            payload: None,
        };

        framing::encode_frame(&frame, &mut self.transport.stream)?;

        self.state = next_state;
        if let framing::Frame::AMQP {
            channel,
            performative,
            payload,
        } = frame
        {
            if let Some(framing::Performative::Open(data)) = performative {
                self.pending_events.push(Event::LocalOpen(data));
            }
        }
        Ok(())
    }

    pub fn create_session() -> Result<Session> {
        Err(AmqpError::NotImplemented)
    }

    pub fn create_sender(&self, _name: String) -> Result<Sender> {
        Err(AmqpError::NotImplemented)
    }

    pub fn create_receiver(&self, _name: String) -> Result<Receiver> {
        Err(AmqpError::NotImplemented)
    }
}

impl Session {
    pub fn create_sender(&self, _name: String) -> Result<Sender> {
        Err(AmqpError::NotImplemented)
    }

    pub fn create_receiver(&self, _name: String) -> Result<Receiver> {
        Err(AmqpError::NotImplemented)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::io;
    use std::thread;
    use std::time;

    #[test]
    fn check_client() {
        let mut cont = Container::new("ce8c4a3e-96b3-11e9-9bfd-c85b7644b4a4");

        let mut conn = cont
            .connect(ConnectionOptions {
                host: "localhost",
                port: 5672,
            })
            .expect("Error opening connection");

        loop {
            let event = conn.next_event();
            match event {
                Ok(Event::ConnectionInit) => {
                    println!("Got initialization event");
                    conn.open();
                }
                Ok(e) => println!("Got event: {:?}", e),
                Err(AmqpError::IoError(ref e)) if e.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(time::Duration::from_millis(100));
                    continue;
                }
                Err(e) => {
                    println!("Got error: {:?}", e);
                    assert!(false);
                }
            }
        }
    }
}
