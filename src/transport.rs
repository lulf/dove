/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use std::convert::From;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::vec::Vec;

use crate::error::*;
use crate::framing::*;
use crate::types::*;

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
            // println!("Filled {} bytes", len);
        }
        // println!("Position is now {}", self.position);
        Ok(&self.buffer[0..self.position])
    }

    pub fn consume(self: &mut Self, nbytes: usize) -> Result<()> {
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
}

impl Transport {
    pub fn new(stream: TcpStream, max_frame_size: usize) -> Result<Transport> {
        // stream.set_nonblocking(true)?;
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
            self.incoming.consume(8)?;
            Ok(Some(remote_version))
        } else {
            Ok(None)
        }
    }

    pub fn read_frame(self: &mut Self) -> Result<Option<Frame>> {
        let mut buf = self.incoming.fill(&mut self.stream)?;
        // println!("Filled {} bytes", buf.len());
        if buf.len() >= 8 {
            let header = decode_header(&mut buf)?;
            let frame_size = header.size as usize;
            /*
            println!(
                "Found enough bytes for header {:?}. Buffer is {} bytes!",
                header,
                buf.len()
            );
            */
            if buf.len() >= frame_size - 8 {
                let frame = decode_frame(header, &mut buf)?;
                self.incoming.consume(frame_size)?;
                Ok(Some(frame))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn write_frame(self: &mut Self, frame: &Frame) -> Result<usize> {
        encode_frame(frame, &mut self.outgoing)
    }

    pub fn write(self: &mut Self, data: &[u8]) -> Result<usize> {
        self.outgoing.write_all(data)?;
        Ok(data.len())
    }

    pub fn flush(self: &mut Self) -> Result<usize> {
        let len = self.outgoing.len();
        self.stream.write_all(self.outgoing.as_mut())?;
        self.outgoing.clear();
        Ok(len)
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
