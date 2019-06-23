/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use byteorder::WriteBytesExt;
use byteorder::{ByteOrder, NetworkEndian};
use std::collections::HashMap;
use std::convert::From;
use std::error;
use std::fmt;
use std::io::Write;
use std::net::TcpStream;
use std::vec::Vec;

type Result<T> = std::result::Result<T, AmqpError>;

#[derive(Clone, Debug, PartialEq)]
pub enum AmqpValue {
    Null,
    Boolean(bool),
    Ubyte(u8),
    Ushort(u16),
    Uint(u32),
    Ulong(u64),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Decimal32(u32),
    Decimal64(u64),
    Decimal128([u64; 2]),
    Char(char),
    Timestamp(i64),
    Uuid([u8; 16]),
    Binary(u32),
    String(String),
    Symbol(&'static str),
    List(Vec<AmqpValue>),
    Map(HashMap<String, AmqpValue>),
    Array(Vec<AmqpValue>),
    Milliseconds(u32),
    IetfLanguageTag(&'static str),
    Fields(HashMap<&'static str, AmqpValue>),
}

fn encode_value(value: &AmqpValue, stream: &Write) -> Result<()> {
    return Ok(());
}

fn encoded_size(value: &AmqpValue) -> u32 {
    return 0;
}

/*
struct Frame<'a> {
    frameType: u8,
    channel: u16,
    extended: &'a[u8],
    payload: &'a[u8]
}
*/

struct Descriptor(&'static str, u64);

struct OpenFrame {
    container_id: AmqpValue,
    hostname: AmqpValue,
    max_frame_size: AmqpValue,
    channel_max: AmqpValue,
    idle_time_out: AmqpValue,
    outgoing_locales: AmqpValue,
    incoming_locales: AmqpValue,
    offered_capabilities: AmqpValue,
    desired_capabilities: AmqpValue,
    properties: AmqpValue,
}

impl OpenFrame {
    fn encode(&self, stream: &mut Write) -> Result<()> {
        let sz = self.size();
        let doff = 2;
        stream.write_u32::<NetworkEndian>(sz);
        stream.write_u8(doff);
        stream.write_u8(0);
        stream.write_u16::<NetworkEndian>(0);
        encode_value(&self.container_id, stream);
        encode_value(&self.hostname, stream);
        return Ok(());
    }

    fn size(&self) -> u32 {
        return 8
            + encoded_size(&self.container_id)
            + encoded_size(&self.hostname)
            + encoded_size(&self.max_frame_size)
            + encoded_size(&self.channel_max)
            + encoded_size(&self.idle_time_out)
            + encoded_size(&self.outgoing_locales)
            + encoded_size(&self.incoming_locales)
            + encoded_size(&self.offered_capabilities)
            + encoded_size(&self.desired_capabilities)
            + encoded_size(&self.properties);
    }
}

/*
fn decode(frame: &Frame, stream: &Read) -> Result<Frame> {
    let size = stream.read_u32()?;
}
*/

#[derive(Debug, Clone)]
pub struct AmqpError {
    msg: String,
}

impl error::Error for AmqpError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

impl fmt::Display for AmqpError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}
/*
impl convert::From for AmqpError {
    fn from(error: k)
}
    */

impl AmqpError {
    fn new(message: &'static str) -> AmqpError {
        AmqpError {
            msg: String::from(message),
        }
    }
}

pub struct ConnectionOptions {
    host: &'static str,
    port: u16,
}

pub struct Container {
    id: &'static str,
}

enum ConnectionState {
    Start,
    HdrRcvd,
    HdrSent,
    OpenPipe,
    OcPipe,
    OpenRcvd,
    OpenSent,
    ClosePipe,
    Opened,
    CloseRcvd,
    CloseSent,
    Discarding,
    End,
}

pub struct Connection {
    stream: TcpStream,
    state: ConnectionState,
}

pub struct Session {}

pub struct Link {}

pub struct Sender {}

pub struct Receiver {}

const AMQP_10_VERSION: [u8; 8] = [65, 77, 81, 80, 0, 1, 0, 0];

impl Container {
    pub fn new(id: &'static str) -> Container {
        Container { id: id }
    }

    pub fn connect(&self, opts: ConnectionOptions) -> std::io::Result<Connection> {
        let mut stream = TcpStream::connect(format!("{}:{}", opts.host, opts.port))?;
        // TODO: SASL support

        stream.write(&AMQP_10_VERSION);

        // AMQP OPEN
        let frame = OpenFrame {
            container_id: AmqpValue::String(String::from(self.id)),
            hostname: AmqpValue::String(String::from(opts.host)),
            max_frame_size: AmqpValue::Uint(4294967295),
            channel_max: AmqpValue::Ushort(65535),
            idle_time_out: AmqpValue::Uint(10000),
            outgoing_locales: AmqpValue::Array(Vec::new()),
            incoming_locales: AmqpValue::Array(Vec::new()),
            offered_capabilities: AmqpValue::Array(Vec::new()),
            desired_capabilities: AmqpValue::Array(Vec::new()),
            properties: AmqpValue::Map(HashMap::new()),
        };

        frame.encode(&mut stream);

        return Ok(Connection {
            stream: stream,
            state: ConnectionState::OpenPipe,
        });
    }

    /*
    pub fn listen(&self, opts: ListenOptions) -> io::Result<()> {
        return Ok(());
    }
    */
}

impl Connection {
    pub fn create_session() -> Result<Session> {
        return Err(AmqpError::new("Not yet implemented"));
    }

    pub fn create_sender(&self, name: String) -> Result<Sender> {
        return Err(AmqpError::new("Not yet implemented"));
    }

    pub fn create_receiver(&self, name: String) -> Result<Receiver> {
        return Err(AmqpError::new("Not yet implemented"));
    }
}

impl Session {
    pub fn create_sender(&self, name: String) -> Result<Sender> {
        return Err(AmqpError::new("Not yet implemented"));
    }

    pub fn create_receiver(&self, name: String) -> Result<Receiver> {
        return Err(AmqpError::new("Not yet implemented"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_connection() {
        let cont = Container::new("myid");

        let conn = cont
            .connect(ConnectionOptions {
                host: "127.0.0.1",
                port: 5672,
            })
            .unwrap();

        println!("YAY!");
    }
}
