/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use std::io::Read;
use std::io::Write;
use std::net::Shutdown;
use std::net::TcpStream;
use std::time::Instant;
use std::vec::Vec;

use crate::error::*;
use crate::framing::*;

#[derive(Debug, PartialEq, Eq)]
pub struct Version(pub u8, pub u8, pub u8);

#[derive(Debug, PartialEq, Eq)]
pub enum ProtocolHeader {
    AMQP(Version),
    SASL(Version),
}

const PROTOCOL_AMQP: [u8; 5] = [65, 77, 81, 80, 0];
const PROTOCOL_SASL: [u8; 5] = [65, 77, 81, 80, 3];

impl ProtocolHeader {
    pub fn decode(reader: &mut dyn Read) -> Result<ProtocolHeader> {
        let mut protocol_type: [u8; 5] = [0; 5];
        reader.read(&mut protocol_type)?;

        let mut protocol_version: [u8; 3] = [0; 3];
        reader.read(&mut protocol_version)?;

        match protocol_type {
            PROTOCOL_AMQP => Ok(ProtocolHeader::AMQP(Version(
                protocol_version[0],
                protocol_version[1],
                protocol_version[2],
            ))),
            PROTOCOL_SASL => Ok(ProtocolHeader::SASL(Version(
                protocol_version[0],
                protocol_version[1],
                protocol_version[2],
            ))),
            _ => Err(AmqpError::Generic(format!(
                "Unknown protocol type {:?}",
                protocol_type
            ))),
        }
    }

    pub fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        let mut header: [u8; 8] = [0; 8];
        match self {
            ProtocolHeader::AMQP(Version(major, minor, micro)) => {
                for i in 0..5 {
                    header[i] = PROTOCOL_AMQP[i];
                }
                header[5] = *major;
                header[6] = *minor;
                header[7] = *micro;
            }
            ProtocolHeader::SASL(Version(major, minor, micro)) => {
                for i in 0..5 {
                    header[i] = PROTOCOL_SASL[i];
                }
                header[5] = *major;
                header[6] = *minor;
                header[7] = *micro;
            }
        }
        writer.write_all(&header[..])?;
        Ok(())
    }
}

#[derive(Debug)]
struct ReadBuffer {
    buffer: Vec<u8>,
    capacity: usize,
    position: usize,
}

impl ReadBuffer {
    fn new(capacity: usize) -> ReadBuffer {
        ReadBuffer {
            buffer: vec![0; capacity],
            capacity: capacity,
            position: 0,
        }
    }

    fn peek(self: &mut Self) -> &[u8] {
        &self.buffer[0..self.position]
    }

    fn fill(self: &mut Self, reader: &mut dyn Read) -> Result<&[u8]> {
        if self.position < self.capacity {
            let len = reader.read(&mut self.buffer[self.position..self.capacity])?;
            self.position += len;
            // println!("Filled {} bytes", len);
        }
        // println!("Position is now {}", self.position);
        Ok(&self.buffer[0..self.position])
    }

    fn consume(self: &mut Self, nbytes: usize) -> Result<()> {
        self.buffer.drain(0..nbytes);
        self.buffer.resize(self.capacity, 0);
        self.position -= nbytes;
        // println!("(Consume) Position is now {}", self.position);
        Ok(())
    }
}

#[derive(Debug)]
pub struct Transport {
    stream: TcpStream,
    incoming: ReadBuffer,
    outgoing: Vec<u8>,
    max_frame_size: usize,
    last_sent: Instant,
    last_received: Instant,
}

impl Transport {
    pub fn new(stream: TcpStream, max_frame_size: usize) -> Result<Transport> {
        stream.set_nonblocking(true)?;
        Ok(Transport {
            stream: stream,
            incoming: ReadBuffer::new(max_frame_size),
            outgoing: Vec::with_capacity(max_frame_size),
            max_frame_size: max_frame_size,
            last_sent: Instant::now(),
            last_received: Instant::now(),
        })
    }

    pub fn close(self: &mut Self) -> Result<()> {
        self.stream.shutdown(Shutdown::Both)?;
        Ok(())
    }

    pub fn read_protocol_header(self: &mut Self) -> Result<Option<ProtocolHeader>> {
        let mut buf = self.incoming.peek();
        if buf.len() >= 8 {
            let header = ProtocolHeader::decode(&mut buf)?;
            self.incoming.consume(8)?;
            Ok(Some(header))
        } else {
            self.incoming.fill(&mut self.stream)?;
            Ok(None)
        }
    }

    pub fn write_protocol_header(self: &mut Self, header: &ProtocolHeader) -> Result<()> {
        header.encode(&mut self.outgoing)?;
        self.flush()?;
        Ok(())
    }

    pub fn read_frame(self: &mut Self) -> Result<Frame> {
        loop {
            let mut buf = self.incoming.peek();
            // println!("Filled {} bytes", buf.len());
            if buf.len() >= 8 {
                let header = FrameHeader::decode(&mut buf)?;
                let frame_size = header.size as usize;
                /*
                println!(
                    "Found enough bytes for header {:?}. Buffer is {} bytes!",
                    header,
                    buf.len()
                );
                */
                if buf.len() >= frame_size - 8 {
                    let frame = Frame::decode(header, &mut buf)?;
                    self.incoming.consume(frame_size)?;
                    self.last_received = Instant::now();
                    println!("RX {:?}", frame);
                    return Ok(frame);
                }
            } else {
                self.incoming.fill(&mut self.stream)?;
            }
        }
    }

    pub fn write_frame(self: &mut Self, frame: &Frame) -> Result<usize> {
        let sz = frame.encode(&mut self.outgoing)?;
        self.last_sent = Instant::now();
        self.flush()?;
        Ok(sz)
    }

    pub fn write(self: &mut Self, data: &[u8]) -> Result<usize> {
        self.outgoing.write_all(data)?;
        self.flush()?;
        Ok(data.len())
    }

    pub fn flush(self: &mut Self) -> Result<usize> {
        let len = self.outgoing.len();
        self.stream.write_all(self.outgoing.as_mut())?;
        self.outgoing.clear();
        Ok(len)
    }

    pub fn last_received(self: &Self) -> Instant {
        self.last_received
    }

    pub fn last_sent(self: &Self) -> Instant {
        self.last_sent
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
