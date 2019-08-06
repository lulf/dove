/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

mod error;
mod framing;
mod types;

use std::convert::From;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::vec::Vec;

pub use error::Result;
pub use error::*;

#[derive(Debug)]
pub struct ReadBuffer {
    buffer: Vec<u8>,
    capacity: usize,
    position: usize,
}

impl ReadBuffer {
    pub fn new(capacity: usize) -> ReadBuffer {
        ReadBuffer {
            buffer: vec![0; capacity],
            capacity: capacity,
            position: 0,
        }
    }

    pub fn fill(self: &mut Self, reader: &mut Read) -> Result<&[u8]> {
        if self.position < self.capacity {
            let len = reader.read(&mut self.buffer[self.position..self.capacity])?;
            self.position += len;
        }
        Ok(&self.buffer[0..self.position])
    }

    pub fn consume(self: &mut Self, nbytes: usize) -> Result<()> {
        self.buffer.drain(0..nbytes);
        self.buffer.resize(self.capacity, 0);
        self.position -= nbytes;
        Ok(())
    }
}

pub struct ConnectionOptions {
    pub host: &'static str,
    pub port: u16,
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

pub struct ConnectionDriver {
    opts: ConnectionOptions,
    pub container_id: String,
    transport: Transport,
    state: ConnectionState,
    pending_events: Vec<Event>,
}

pub struct Transport {
    stream: TcpStream,
    incoming: ReadBuffer,
    outgoing: Vec<u8>,
    max_frame_size: usize,
}

impl Transport {
    pub fn new(stream: TcpStream, max_frame_size: usize) -> Result<Transport> {
        stream.set_nonblocking(true)?;
        Ok(Transport {
            stream: stream,
            incoming: ReadBuffer::new(max_frame_size),
            outgoing: Vec::with_capacity(max_frame_size),
            max_frame_size: max_frame_size,
        })
    }

    pub fn read_protocol_version(self: &mut Self) -> Result<Option<[u8; 8]>> {
        let mut buf = self.incoming.fill(&mut self.stream)?;
        if buf.len() >= 8 {
            let mut remote_version: [u8; 8] = [0; 8];
            buf.read(&mut remote_version)?;
            Ok(Some(remote_version))
        } else {
            Ok(None)
        }
    }

    pub fn read_frame(self: &mut Self) -> Result<Option<framing::Frame>> {
        let mut buf = self.incoming.fill(&mut self.stream)?;
        Ok(None)
    }

    pub fn write_frame(self: &mut Self, frame: &framing::Frame) -> Result<usize> {
        framing::encode_frame(frame, &mut self.outgoing)
    }

    pub fn write(self: &mut Self, data: &[u8]) -> Result<usize> {
        self.outgoing.write_all(data)?;
        Ok(data.len())
    }

    pub fn flush(self: &mut Self) -> Result<usize> {
        let len = self.outgoing.len();
        self.stream.write_all(self.outgoing.as_mut())?;
        Ok(len)
    }
}

impl Container {
    pub fn new(id: &'static str) -> Container {
        Container {
            id: String::from(id),
        }
    }

    pub fn connect(&self, opts: ConnectionOptions) -> Result<ConnectionDriver> {
        let stream = TcpStream::connect(format!("{}:{}", opts.host, opts.port))?;
        // TODO: SASL support
        let transport: Transport = Transport::new(stream, 1024)?;

        let driver = ConnectionDriver {
            container_id: self.id.clone(),
            transport: transport,
            pending_events: Vec::new(),

            opts: opts,
            state: ConnectionState::Start,
        };

        Ok(driver)
    }
}

/*
pub fn listen(&self, opts: ListenOptions) -> io::Result<()> {
    return Ok(());
}
*/

struct Context<'a> {
    connection: Option<&'a ConnectionDriver>,
}

#[derive(Debug)]
pub enum Event {
    ConnectionInit,
    RemoteOpen(framing::Open),
    LocalOpen(framing::Open),
    UnknownFrame(framing::Frame),
    Transport,
}

impl ConnectionDriver {
    // Wait for next state event
    pub fn next_event(self: &mut Self) -> Result<Option<Event>> {
        if self.pending_events.len() > 0 {
            Ok(Some(self.pending_events.remove(0)))
        } else {
            println!("Checking state...");
            match self.state {
                ConnectionState::Start => {
                    self.transport.write(&AMQP_10_VERSION)?;
                    self.transport.flush()?;
                    self.state = ConnectionState::HdrSent;
                    self.next_event()
                }
                ConnectionState::HdrSent => {
                    println!("Reading remote version...");
                    let mut remote_version = self.transport.read_protocol_version()?;
                    println!("Done!");
                    println!("Received remote version buf: {:?}", remote_version);
                    self.state = ConnectionState::HdrExch;
                    Ok(Some(Event::ConnectionInit))
                }
                ConnectionState::HdrRcvd => {
                    self.transport.write(&AMQP_10_VERSION)?;
                    self.transport.flush()?;
                    self.state = ConnectionState::HdrExch;
                    Ok(Some(Event::ConnectionInit))
                }
                ConnectionState::HdrExch => {
                    let frame = self.transport.read_frame()?;
                    if let Some(f) = frame {
                        self.handle_open(f, ConnectionState::OpenSent)
                    } else {
                        Ok(None)
                    }
                }
                ConnectionState::OpenSent => {
                    let frame = self.transport.read_frame()?;
                    if let Some(f) = frame {
                        self.handle_open(f, ConnectionState::Opened)
                    } else {
                        Ok(None)
                    }
                }
                ConnectionState::Opened => {
                    let frame = self.transport.read_frame()?;
                    Ok(None)
                }
                _ => Err(AmqpError::NotImplemented),
            }
        }
    }

    fn handle_open(
        self: &mut Self,
        frame: framing::Frame,
        next_state: ConnectionState,
    ) -> Result<Option<Event>> {
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
                        return Ok(Some(Event::RemoteOpen(open)));
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

    use super::ReadBuffer;
    use std::io::Read;

    #[test]
    fn readbuffer() {
        let mut buf = ReadBuffer::new(6);

        let input: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        let data = buf.fill(&mut &input[..]).expect("Unable to fill buffer");

        assert_eq!(6, data.len());
        assert_eq!([1, 2, 3, 4, 5, 6], data);

        let data = buf.fill(&mut &input[..]).expect("Unable to fill buffer");
        assert_eq!(6, data.len());
        assert_eq!([1, 2, 3, 4, 5, 6], data);

        buf.consume(1).expect("Unable to consume bytes");

        let data = buf.fill(&mut &input[6..]).expect("Unable to fill buffer");
        assert_eq!(6, data.len());
        assert_eq!([2, 3, 4, 5, 6, 7], data);
    }
}
