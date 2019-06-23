/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use byteorder::NetworkEndian;
use byteorder::WriteBytesExt;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::convert::From;
use std::error;
use std::fmt;
use std::io;
use std::io::Write;
use std::net::TcpStream;
use std::vec::Vec;

type Result<T> = std::result::Result<T, AmqpError>;

#[derive(Clone, Debug)]
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
    Map(BTreeMap<AmqpValue, AmqpValue>),
    Array(Vec<AmqpValue>),
    Milliseconds(u32),
    IetfLanguageTag(&'static str),
    Fields(BTreeMap<&'static str, AmqpValue>),
}

impl std::cmp::Eq for AmqpValue {}

impl std::cmp::PartialOrd for AmqpValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        return None;
    }
}

impl std::cmp::Ord for AmqpValue {
    fn cmp(&self, other: &Self) -> Ordering {
        return Ordering::Equal;
    }
}

impl std::cmp::PartialEq for AmqpValue {
    fn eq(&self, other: &Self) -> bool {
        return true;
    }
}

const TYPE_CODE_NULL: u8 = 0x40;
const TYPE_CODE_BOOL: u8 = 0x56;
const TYPE_CODE_TRUEBOOL: u8 = 0x41;
const TYPE_CODE_FALSEBOOL: u8 = 0x42;
const TYPE_CODE_UBYTE: u8 = 0x50;
const TYPE_CODE_USHORT: u8 = 0x60;
const TYPE_CODE_UINT: u8 = 0x70;
const TYPE_CODE_SMALLUINT: u8 = 0x52;
const TYPE_CODE_UINT0: u8 = 0x43;
const TYPE_CODE_ULONG: u8 = 0x80;
const TYPE_CODE_SMALLULONG: u8 = 0x53;
const TYPE_CODE_ULONG0: u8 = 0x44;
const TYPE_CODE_BYTE: u8 = 0x51;
const TYPE_CODE_SHORT: u8 = 0x61;
const TYPE_CODE_SMALLINT: u8 = 0x54;
const TYPE_CODE_INT: u8 = 0x71;
const TYPE_CODE_SMALLLONG: u8 = 0x55;
const TYPE_CODE_LONG: u8 = 0x81;
const TYPE_CODE_FLOAT: u8 = 0x72;
const TYPE_CODE_DOUBLE: u8 = 0x82;
const TYPE_CODE_DECIMAL32: u8 = 0x74;
const TYPE_CODE_DECIMAL64: u8 = 0x84;
const TYPE_CODE_DECIMAL128: u8 = 0x94;
const TYPE_CODE_CHAR: u8 = 0x73;
const TYPE_CODE_TIMESTAMP: u8 = 0x83;
const TYPE_CODE_UUID: u8 = 0x98;
const TYPE_CODE_VBIN8: u8 = 0xA0;
const TYPE_CODE_VBIN32: u8 = 0xB0;
const TYPE_CODE_STR8: u8 = 0xA1;
const TYPE_CODE_STR32: u8 = 0xB1;
const TYPE_CODE_SYM8: u8 = 0xA3;
const TYPE_CODE_SYM32: u8 = 0xB3;
const TYPE_CODE_LIST0: u8 = 0x45;
const TYPE_CODE_LIST8: u8 = 0xC0;
const TYPE_CODE_LIST32: u8 = 0xD0;
const TYPE_CODE_MAP8: u8 = 0xC1;
const TYPE_CODE_MAP32: u8 = 0xD1;
const TYPE_CODE_ARRAY8: u8 = 0xE0;
const TYPE_CODE_ARRAY32: u8 = 0xF0;

fn encode_value(value: &AmqpValue, stream: &Write) -> Result<()> {
    match value {
        AmqpValue::Null => stream.write_u8(TYPE_CODE_NULL)?,
        AmqpValue::Boolean(value) => {
            // stream.write_u8(TYPE_CODE_BOOL)?;
            stream.write_u8(if *value { 0x41 } else { 0x42 })?;
        }
        AmqpValue::Ubyte(value) => {
            stream.write_u8(TYPE_CODE_UBYTE)?;
            stream.write_u8(*value)?;
        }
        AmqpValue::Ushort(value) => {
            stream.write_u8(TYPE_CODE_USHORT)?;
            stream.write_u16::<NetworkEndian>(*value)?;
        }
        AmqpValue::Uint(value) => {
            stream.write_u8(TYPE_CODE_UINT)?;
            stream.write_u32::<NetworkEndian>(*value)?;
        }
        AmqpValue::Ulong(value) => {
            stream.write_u8(TYPE_CODE_ULONG)?;
            stream.write_u64::<NetworkEndian>(*value)?;
        }
        AmqpValue::Byte(value) => {
            stream.write_u8(TYPE_CODE_BYTE)?;
            stream.write_i8(*value)?;
        }
        AmqpValue::Short(value) => {
            stream.write_u8(TYPE_CODE_SHORT)?;
            stream.write_i16::<NetworkEndian>(*value)?;
        }
        AmqpValue::Int(value) => {
            stream.write_u8(TYPE_CODE_INT)?;
            stream.write_i32::<NetworkEndian>(*value)?;
        }
        AmqpValue::Long(value) => {
            stream.write_u8(TYPE_CODE_LONG)?;
            stream.write_i64::<NetworkEndian>(*value)?;
        }
        AmqpValue::Float(value) => {
            stream.write_u8(TYPE_CODE_FLOAT)?;
            stream.write_f32::<NetworkEndian>(*value)?;
        }
        AmqpValue::Double(value) => {
            stream.write_u8(TYPE_CODE_DOUBLE)?;
            stream.write_f64::<NetworkEndian>(*value)?;
        }
        AmqpValue::Decimal32(value) => {
            stream.write_u8(TYPE_CODE_DECIMAL32)?;
            stream.write_u32::<NetworkEndian>(*value)?;
        }
        AmqpValue::Decimal64(value) => {
            stream.write_u8(TYPE_CODE_DECIMAL64)?;
            stream.write_u64::<NetworkEndian>(*value)?;
        }
        AmqpValue::Decimal128(value) => {
            stream.write_u8(TYPE_CODE_DECIMAL128)?;
            //stream.write_all(&value)?;
        }
    }
    return Ok(());
}

fn encoded_size(_value: &AmqpValue) -> u32 {
    return 0;
}

struct Descriptor(&'static str, u64);

const DESC_OPEN: Descriptor = Descriptor("amqp:open:list", 0x0000_0000_0000_0010);

fn encode_descriptor(descriptor: Descriptor, stream: &mut Write) -> Result<()> {
    let Descriptor(_, code) = descriptor;
    stream.write_u64::<NetworkEndian>(code)?;
    return Ok(());
}

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
        stream.write_u32::<NetworkEndian>(sz)?;
        stream.write_u8(doff)?;
        stream.write_u8(0)?;
        stream.write_u16::<NetworkEndian>(0)?;
        encode_descriptor(DESC_OPEN, stream)?;
        encode_value(&self.container_id, stream)?;
        encode_value(&self.hostname, stream)?;
        encode_value(&self.max_frame_size, stream)?;
        encode_value(&self.channel_max, stream)?;
        encode_value(&self.idle_time_out, stream)?;
        encode_value(&self.outgoing_locales, stream)?;
        encode_value(&self.incoming_locales, stream)?;
        encode_value(&self.offered_capabilities, stream)?;
        encode_value(&self.desired_capabilities, stream)?;
        encode_value(&self.properties, stream)?;
        return Ok(());
    }

    fn size(&self) -> u32 {
        return 8
            + 8
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

impl std::convert::From<io::Error> for AmqpError {
    fn from(error: io::Error) -> Self {
        return AmqpError {
            msg: error.to_string(),
        };
    }
}

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
    //Start,
    //HdrRcvd,
    //HdrSent,
    OpenPipe,
    //OcPipe,
    //OpenRcvd,
    //OpenSent,
    //ClosePipe,
    //Opened,
    //CloseRcvd,
    //CloseSent,
    //Discarding,
    //End,
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

    pub fn connect(&self, opts: ConnectionOptions) -> Result<Connection> {
        let mut stream = TcpStream::connect(format!("{}:{}", opts.host, opts.port))?;
        // TODO: SASL support

        stream.write(&AMQP_10_VERSION)?;

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
            properties: AmqpValue::Map(BTreeMap::new()),
        };

        frame.encode(&mut stream)?;

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

    pub fn create_sender(&self, _name: String) -> Result<Sender> {
        return Err(AmqpError::new("Not yet implemented"));
    }

    pub fn create_receiver(&self, _name: String) -> Result<Receiver> {
        return Err(AmqpError::new("Not yet implemented"));
    }
}

impl Session {
    pub fn create_sender(&self, _name: String) -> Result<Sender> {
        return Err(AmqpError::new("Not yet implemented"));
    }

    pub fn create_receiver(&self, _name: String) -> Result<Receiver> {
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
