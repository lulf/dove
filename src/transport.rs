/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The transport module contains the network connectivity transport for the upper layers. It is implemented using mio.

use std::fmt::Debug;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;

use std::time::Instant;

use crate::error::*;
use crate::framing::*;
use std::sync::{Arc, Mutex};

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
        reader.read_exact(&mut protocol_type)?;

        let mut protocol_version: [u8; 3] = [0; 3];
        reader.read_exact(&mut protocol_version)?;

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
                header[..5].clone_from_slice(&PROTOCOL_AMQP[..5]);
                header[5] = *major;
                header[6] = *minor;
                header[7] = *micro;
            }
            ProtocolHeader::SASL(Version(major, minor, micro)) => {
                header[..5].clone_from_slice(&PROTOCOL_SASL[..5]);
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
struct Buffer {
    buffer: Vec<u8>,
    position: usize,
}

impl Buffer {
    fn new(capacity: usize) -> Buffer {
        Buffer {
            buffer: vec![0u8; capacity],
            position: 0,
        }
    }

    fn peek(&mut self) -> &[u8] {
        &self.buffer[0..self.position]
    }

    fn fill(&mut self, reader: &mut dyn Read) -> Result<&[u8]> {
        let capacity = self.buffer.capacity();
        if self.position < capacity {
            let len = reader.read(&mut self.buffer[self.position..capacity])?;

            if len == 0 && capacity.saturating_sub(self.position) > 0 {
                return Err(AmqpError::IoError(std::io::Error::from(
                    std::io::ErrorKind::UnexpectedEof,
                )));
            }
            self.position += len;
            // println!("Filled {} bytes", len);
        }
        // println!("Position is now {}", self.position);
        Ok(&self.buffer[0..self.position])
    }

    fn consume(&mut self, nbytes: usize) {
        // TODO: Should ideally avoid this copy by making this a circular buffer.
        self.buffer.rotate_left(nbytes);
        self.position -= nbytes;
        // println!("(Consume) Position is now {}", self.position);
    }

    fn written(&mut self) -> &[u8] {
        &self.buffer[0..self.position]
    }

    fn clear(&mut self) {
        self.position = 0;
    }

    fn write_buf(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if data.len() > self.buffer.capacity().saturating_sub(self.position) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "written data is bigger than output buffer",
            ));
        }
        // println!("Copying {} bytes into {}", data.len(), self.position);
        self.buffer[self.position..(self.position + data.len())].clone_from_slice(data);
        self.position += data.len();
        Ok(data.len())
    }
}

impl Write for Buffer {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.write_buf(data)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub trait Network: Read + Write + Debug {
    fn close(&mut self) -> Result<()>;
}

pub struct TransportInfo {
    last_sent: Mutex<Instant>,
    last_received: Mutex<Instant>,
}

impl TransportInfo {
    fn update_last_sent(&self) {
        *self.last_sent.lock().unwrap() = Instant::now();
    }

    pub fn last_sent(&self) -> Instant {
        *self.last_sent.lock().unwrap()
    }

    fn update_last_received(&self) {
        *self.last_received.lock().unwrap() = Instant::now();
    }

    pub fn last_received(&self) -> Instant {
        *self.last_received.lock().unwrap()
    }
}

impl Debug for TransportInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("TransportInfo")
            .field("last_sent", &self.last_sent())
            .field("last_received", &self.last_received())
            .finish_non_exhaustive()
    }
}

impl Default for TransportInfo {
    fn default() -> Self {
        let now = Instant::now();
        TransportInfo {
            last_sent: Mutex::new(now),
            last_received: Mutex::new(now),
        }
    }
}

#[derive(Debug)]
pub struct Transport<N: Network> {
    network: N,
    incoming: Buffer,
    outgoing: Buffer,
    _max_frame_size: usize,
    info: Arc<TransportInfo>,
}

impl<N: Network> Transport<N> {
    pub fn new(network: N, max_frame_size: usize) -> Transport<N> {
        Transport {
            network,
            incoming: Buffer::new(max_frame_size),
            outgoing: Buffer::new(max_frame_size),
            _max_frame_size: max_frame_size,
            info: Arc::new(TransportInfo::default()),
        }
    }

    pub fn info(&self) -> &Arc<TransportInfo> {
        &self.info
    }

    pub fn network(&self) -> &N {
        &self.network
    }

    pub fn network_mut(&mut self) -> &mut N {
        &mut self.network
    }

    pub fn close(&mut self) -> Result<()> {
        self.network.close()?;
        Ok(())
    }

    pub fn read_protocol_header(&mut self) -> Result<Option<ProtocolHeader>> {
        let mut buf = self.incoming.peek();
        if buf.len() >= 8 {
            let header = ProtocolHeader::decode(&mut buf)?;
            self.incoming.consume(8);
            Ok(Some(header))
        } else {
            self.incoming.fill(&mut self.network)?;
            Ok(None)
        }
    }

    pub fn write_protocol_header(&mut self, header: &ProtocolHeader) -> Result<()> {
        header.encode(&mut self.outgoing)?;
        Ok(())
    }

    pub fn read_frame(&mut self) -> Result<Frame> {
        loop {
            let mut buf = self.incoming.peek();
            trace!("Filled {} bytes", buf.len());
            if buf.len() >= 8 {
                let header = FrameHeader::decode(&mut buf)?;
                let frame_size = header.size as usize;
                trace!(
                    "Found enough bytes for header {:?}. Buffer is {} bytes!",
                    header,
                    buf.len()
                );
                if buf.len() >= frame_size - 8 {
                    let mut cursor = Cursor::new(&mut buf);
                    let frame = Frame::decode(header, &mut cursor)?;
                    self.incoming.consume(frame_size);
                    self.info.update_last_received();
                    debug!("RX {:?}", frame);
                    return Ok(frame);
                } else {
                    self.incoming.fill(&mut self.network)?;
                }
            } else {
                self.incoming.fill(&mut self.network)?;
            }
        }
    }

    pub fn write_frame(&mut self, frame: &Frame) -> Result<usize> {
        let sz = frame.encode(&mut self.outgoing)?;
        self.info.update_last_sent();
        self.flush()?;
        Ok(sz)
    }

    pub fn write(&mut self, data: &[u8]) -> Result<usize> {
        self.outgoing.write_all(data)?;
        self.flush()?;
        Ok(data.len())
    }

    pub fn flush(&mut self) -> Result<usize> {
        let data = self.outgoing.written();
        let len = data.len();
        self.network.write_all(data)?;
        self.outgoing.clear();
        Ok(len)
    }
}

pub mod mio {
    use mio::event::Source;
    use mio::net::TcpStream;
    use mio::{Interest, Poll, Registry, Token};

    use super::Network;
    use crate::error::*;
    use std::io::Read;
    use std::io::Write;
    use std::net::ToSocketAddrs;
    use std::net::{Shutdown, SocketAddr};

    #[derive(Debug)]
    pub struct MioNetwork {
        stream: TcpStream,
        peer: SocketAddr,
    }

    impl MioNetwork {
        pub fn connect<S: ToSocketAddrs>(host: &S) -> Result<MioNetwork> {
            let mut addrs = host.to_socket_addrs()?;
            let address = addrs
                .next()
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::AddrNotAvailable))?;
            Ok(MioNetwork {
                stream: TcpStream::connect(address)?,
                peer: address,
            })
        }

        pub fn peer_addr(&self) -> SocketAddr {
            self.peer
        }

        pub fn register(&mut self, id: Token, poll: &mut Poll) -> Result<()> {
            poll.registry().register(
                &mut self.stream,
                id,
                Interest::READABLE | Interest::WRITABLE,
            )?;
            Ok(())
        }
    }

    impl Network for MioNetwork {
        fn close(&mut self) -> Result<()> {
            self.stream.shutdown(Shutdown::Both)?;
            Ok(())
        }
    }

    impl Write for MioNetwork {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.stream.write(data)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.stream.flush()
        }
    }

    impl Read for MioNetwork {
        fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
            self.stream.read(b)
        }
    }

    impl Source for MioNetwork {
        fn register(
            &mut self,
            registry: &Registry,
            token: Token,
            interests: Interest,
        ) -> std::io::Result<()> {
            self.stream.register(registry, token, interests)
        }

        fn reregister(
            &mut self,
            registry: &Registry,
            token: Token,
            interests: Interest,
        ) -> std::io::Result<()> {
            self.stream.reregister(registry, token, interests)
        }

        fn deregister(&mut self, registry: &Registry) -> std::io::Result<()> {
            self.stream.deregister(registry)
        }
    }
}

#[cfg(test)]
mod tests {

    use super::Buffer;

    #[test]
    fn readbuffer() {
        let mut buf = Buffer::new(6);

        let input: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        let data = buf.fill(&mut &input[..]).expect("Unable to fill buffer");

        assert_eq!(6, data.len());
        assert_eq!([1, 2, 3, 4, 5, 6], data);

        let data = buf.fill(&mut &input[..]).expect("Unable to fill buffer");
        assert_eq!(6, data.len());
        assert_eq!([1, 2, 3, 4, 5, 6], data);

        buf.consume(1);

        let data = buf.fill(&mut &input[6..]).expect("Unable to fill buffer");
        assert_eq!(6, data.len());
        assert_eq!([2, 3, 4, 5, 6, 7], data);
    }

    #[test]
    fn writebuffer() {
        let mut buf = Buffer::new(6);
        let result = buf.write_buf(&[1, 2, 3, 4, 5, 6, 7]);
        assert!(result.is_err());
        assert_eq!([0, 0, 0, 0, 0, 0], &buf.buffer[..buf.buffer.capacity()]);

        let result = buf.write_buf(&[1, 2, 3, 4]);
        assert!(result.is_ok());
        assert_eq!(4, result.unwrap());
        assert_eq!([1, 2, 3, 4, 0, 0], &buf.buffer[..buf.buffer.capacity()]);

        let result = buf.write_buf(&[5, 6]);
        assert!(result.is_ok());
        assert_eq!(2, result.unwrap());
        assert_eq!([1, 2, 3, 4, 5, 6], &buf.buffer[..buf.buffer.capacity()]);

        let result = buf.write_buf(&[7]);
        assert!(result.is_err());
        assert_eq!([1, 2, 3, 4, 5, 6], &buf.buffer[..buf.buffer.capacity()]);
    }
}
