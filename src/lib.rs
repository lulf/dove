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
    Binary(Box<[u8]>),
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

#[derive(Copy, Clone)]
enum TypeCode {
    Null = 0x40,
    Bool = 0x56,
    Truebool = 0x41,
    Falsebool = 0x42,
    Ubyte = 0x50,
    Ushort = 0x60,
    Uint = 0x70,
    Smalluint = 0x52,
    Uint0 = 0x43,
    Ulong = 0x80,
    Smallulong = 0x53,
    Ulong0 = 0x44,
    Byte = 0x51,
    Short = 0x61,
    Smallint = 0x54,
    Int = 0x71,
    Smalllong = 0x55,
    Long = 0x81,
    Float = 0x72,
    Double = 0x82,
    Decimal32 = 0x74,
    Decimal64 = 0x84,
    Decimal128 = 0x94,
    Char = 0x73,
    Timestamp = 0x83,
    Uuid = 0x98,
    Vbin8 = 0xA0,
    Vbin32 = 0xB0,
    Str8 = 0xA1,
    Str32 = 0xB1,
    Sym8 = 0xA3,
    Sym32 = 0xB3,
    List0 = 0x45,
    List8 = 0xC0,
    List32 = 0xD0,
    Map8 = 0xC1,
    Map32 = 0xD1,
    Array8 = 0xE0,
    Array32 = 0xF0,
}

fn encode_type(code: TypeCode, stream: &mut Write) -> Result<()> {
    stream.write_u8(code as u8)?;
    return Ok(());
}

fn encode_value(amqp_value: &AmqpValue, stream: &mut Write) -> Result<()> {
    match amqp_value {
        AmqpValue::Null => encode_type(TypeCode::Null, stream)?,
        AmqpValue::Boolean(value) => {
            encode_type(
                if *value {
                    TypeCode::Truebool
                } else {
                    TypeCode::Falsebool
                },
                stream,
            )?;
        }
        AmqpValue::Ubyte(value) => {
            encode_type(TypeCode::Ubyte, stream)?;
            stream.write_u8(*value)?;
        }
        AmqpValue::Ushort(value) => {
            encode_type(TypeCode::Ushort, stream)?;
            stream.write_u16::<NetworkEndian>(*value)?;
        }
        AmqpValue::Uint(value) => {
            encode_type(TypeCode::Uint, stream)?;
            stream.write_u32::<NetworkEndian>(*value)?;
        }
        AmqpValue::Ulong(value) => {
            if *value > 0xFF {
                encode_type(TypeCode::Ulong, stream)?;
                stream.write_u64::<NetworkEndian>(*value)?;
            } else if *value > 0 {
                encode_type(TypeCode::Smallulong, stream)?;
                stream.write_u8(*value as u8);
            } else {
                encode_type(TypeCode::Ulong0, stream)?;
            }
        }
        AmqpValue::Byte(value) => {
            encode_type(TypeCode::Byte, stream)?;
            stream.write_i8(*value)?;
        }
        AmqpValue::Short(value) => {
            encode_type(TypeCode::Short, stream)?;
            stream.write_i16::<NetworkEndian>(*value)?;
        }
        AmqpValue::Int(value) => {
            encode_type(TypeCode::Int, stream)?;
            stream.write_i32::<NetworkEndian>(*value)?;
        }
        AmqpValue::Long(value) => {
            encode_type(TypeCode::Long, stream)?;
            stream.write_i64::<NetworkEndian>(*value)?;
        }
        AmqpValue::Float(value) => {
            encode_type(TypeCode::Float, stream)?;
            stream.write_f32::<NetworkEndian>(*value)?;
        }
        AmqpValue::Double(value) => {
            encode_type(TypeCode::Double, stream)?;
            stream.write_f64::<NetworkEndian>(*value)?;
        }
        AmqpValue::Decimal32(value) => {
            encode_type(TypeCode::Decimal32, stream)?;
            stream.write_u32::<NetworkEndian>(*value)?;
        }
        AmqpValue::Decimal64(value) => {
            encode_type(TypeCode::Decimal64, stream)?;
            stream.write_u64::<NetworkEndian>(*value)?;
        }
        AmqpValue::Decimal128(value) => {
            encode_type(TypeCode::Decimal128, stream)?;
            stream.write_u64::<NetworkEndian>(value[0])?;
            stream.write_u64::<NetworkEndian>(value[1])?;
        }
        AmqpValue::Char(value) => {
            encode_type(TypeCode::Char, stream)?;
            // stream.write_u32::<NetworkEndian>(value)?;
        }
        AmqpValue::Timestamp(value) => {
            encode_type(TypeCode::Timestamp, stream)?;
            stream.write_i64::<NetworkEndian>(*value)?;
        }
        AmqpValue::Uuid(value) => {
            encode_type(TypeCode::Uuid, stream)?;
            stream.write(value)?;
        }
        AmqpValue::Binary(value) => {
            if (*value).len() > 0xFFFFFFFF {
                return Err(AmqpError::new(
                    "Binary values cannot be longer than 4294967295 octets",
                ));
            } else if (*value).len() > 0xFF {
                encode_type(TypeCode::Vbin32, stream)?;
                stream.write_u32::<NetworkEndian>((*value).len() as u32)?;
                stream.write(&*value)?;
            } else {
                encode_type(TypeCode::Vbin8, stream)?;
                stream.write_u8((*value).len() as u8)?;
                stream.write(&*value)?;
            }
        }
        AmqpValue::String(value) => {
            if value.len() > 0xFFFFFFFF {
                return Err(AmqpError::new(
                    "String values cannot be longer than 4294967295 octets",
                ));
            } else if value.len() > 0xFF {
                encode_type(TypeCode::Str32, stream)?;
                stream.write_u32::<NetworkEndian>(value.len() as u32);
                stream.write(value.as_bytes())?;
            } else {
                encode_type(TypeCode::Str8, stream)?;
                stream.write_u8(value.len() as u8);
                stream.write(value.as_bytes())?;
            }
        }
        AmqpValue::Symbol(value) => {
            if value.len() > 0xFFFFFFFF {
                return Err(AmqpError::new(
                    "Symbol values cannot be longer than 4294967295 octets",
                ));
            } else if value.len() > 0xFF {
                encode_type(TypeCode::Sym32, stream)?;
                stream.write_u32::<NetworkEndian>(value.len() as u32);
                stream.write(value.as_bytes())?;
            } else {
                encode_type(TypeCode::Sym8, stream)?;
                stream.write_u8(value.len() as u8);
                stream.write(value.as_bytes())?;
            }
        }
        AmqpValue::List(value) => {
            if value.len() > 0xFFFFFFFF {
                return Err(AmqpError::new(
                    "List length cannot be longer than 4294967295 items",
                ));
            } else if value.len() > 0 {
                encode_type(TypeCode::List32, stream)?;
                stream.write_u32::<NetworkEndian>(encoded_size(amqp_value) - 5)?;
                stream.write_u32::<NetworkEndian>(value.len() as u32)?;
                for v in value.iter() {
                    encode_value(&v, stream)?;
                }
            /*
            } else if value.len() > 0 {
                encode_type(TypeCode::List8, stream)?;
                stream.write_u8((encoded_size(amqp_value) - 2) as u8)?;
                stream.write_u8(value.len() as u8)?;
                for v in value.iter() {
                    encode_value(&v, stream)?;
                }
                */
            } else {
                encode_type(TypeCode::List0, stream)?;
            }
        }
        AmqpValue::Map(value) => {
            encode_type(TypeCode::Null, stream)?;
        }
        AmqpValue::Array(value) => {
            if value.len() > 0xFFFFFFFF {
                return Err(AmqpError::new(
                    "Array length cannot be longer than 4294967295 bytes",
                ));
            } else if value.len() > 0xFF {
                encode_type(TypeCode::Array32, stream)?;
            } else if value.len() > 0 {
                encode_type(TypeCode::Array8, stream)?;
            } else {
                encode_type(TypeCode::Null, stream)?;
            }
        }
        AmqpValue::Milliseconds(value) => {
            encode_type(TypeCode::Uint, stream)?;
            stream.write_u32::<NetworkEndian>(*value)?;
        }
        AmqpValue::IetfLanguageTag(value) => {}
        AmqpValue::Fields(value) => {}
    }
    return Ok(());
}

fn encoded_size(value: &AmqpValue) -> u32 {
    return match value {
        AmqpValue::Null => 1,
        AmqpValue::Boolean(value) => 1,
        AmqpValue::Ubyte(value) => 2,
        AmqpValue::Ushort(value) => 3,
        AmqpValue::Uint(value) => 5,
        AmqpValue::Ulong(value) => {
            if *value > 0xFF {
                9
            } else if *value > 0 {
                2
            } else {
                1
            }
        }
        AmqpValue::Byte(value) => 2,
        AmqpValue::Short(value) => 3,
        AmqpValue::Int(value) => 5,
        AmqpValue::Long(value) => 9,
        AmqpValue::Float(value) => 5,
        AmqpValue::Double(value) => 9,
        AmqpValue::Decimal32(value) => 5,
        AmqpValue::Decimal64(value) => 9,
        AmqpValue::Decimal128(value) => 17,
        AmqpValue::Char(value) => 1, // TODO: Fix size
        AmqpValue::Timestamp(value) => 9,
        AmqpValue::Uuid(value) => 1 + value.len() as u32,
        AmqpValue::Binary(value) => {
            if (*value).len() > 0xFF {
                5 + (*value).len() as u32
            } else {
                2 + (*value).len() as u32
            }
        }
        AmqpValue::String(value) => {
            if value.len() > 0xFF {
                5 + value.len() as u32
            } else {
                2 + value.len() as u32
            }
        }
        AmqpValue::Symbol(value) => {
            if value.len() > 0xFF {
                5 + value.len() as u32
            } else {
                2 + value.len() as u32
            }
        }
        AmqpValue::List(value) => {
            if value.len() > 0 {
                9 + value.iter().fold(0, |a, v| a + encoded_size(v))
            /*            } else if value.len() > 0 {
            3 + value.iter().fold(0, |a, v| a + encoded_size(v))*/
            } else {
                1
            }
        }
        AmqpValue::Map(value) => 1,
        AmqpValue::Array(value) => 1,
        AmqpValue::Milliseconds(value) => 5,
        AmqpValue::IetfLanguageTag(value) => 0,
        AmqpValue::Fields(value) => 0,
    };
}

struct Descriptor(&'static str, u64);

const DESC_OPEN: Descriptor = Descriptor("amqp:open:list", 0x0000_0000_0000_0010);

#[derive(Copy, Clone)]
enum Performative {
    Open = 0x10,
}

fn encode_performative(performative: Performative, stream: &mut Write) -> Result<()> {
    stream.write_u8(0);
    encode_value(&AmqpValue::Ulong(performative as u64), stream)?;
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
        let arguments: Vec<AmqpValue> = vec![
            self.container_id.clone(),
            self.hostname.clone(),
            self.max_frame_size.clone(),
            self.channel_max.clone(),
            self.idle_time_out.clone(),
            self.outgoing_locales.clone(),
            self.incoming_locales.clone(),
            self.offered_capabilities.clone(),
            self.desired_capabilities.clone(),
            self.properties.clone(),
        ];

        let list = AmqpValue::List(vec![
            AmqpValue::String(String::from("fab6c474-983c-11e9-98ba-c85b7644b4a4")),
            AmqpValue::String(String::from("localhost")),
            AmqpValue::Null,
            AmqpValue::Ushort(32767),
        ]);
        let performative = AmqpValue::Ulong(Performative::Open as u64);
        let sz = 8 + encoded_size(&list) + encoded_size(&performative) + 1;
        println!("Total size: {}", sz);
        let doff = 2;

        stream.write_u32::<NetworkEndian>(sz)?;
        stream.write_u8(doff)?;
        stream.write_u8(0)?;
        stream.write_u16::<NetworkEndian>(0)?;
        encode_performative(Performative::Open, stream)?;

        encode_value(&list, stream)?;
        return Ok(());
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
            max_frame_size: AmqpValue::Null,
            channel_max: AmqpValue::Ushort(32767),
            idle_time_out: AmqpValue::Null,
            outgoing_locales: AmqpValue::Null,
            incoming_locales: AmqpValue::Null,
            offered_capabilities: AmqpValue::Null,
            desired_capabilities: AmqpValue::Null,
            properties: AmqpValue::Null,
        };

        frame.encode(&mut stream)?;
        stream.flush();

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
    fn encode_list() {
        let list = AmqpValue::List(vec![
            AmqpValue::String(String::from("fab6c474-983c-11e9-98ba-c85b7644b4a4")),
            AmqpValue::String(String::from("localhost")),
            AmqpValue::Null,
            AmqpValue::Ushort(32767),
        ]);

        let mut output: Vec<u8> = Vec::new();
        encode_value(&list, &mut output);
        let sz = encoded_size(&list);
        println!("Size: {}, of output: {}", sz, output.len());
        println!("{:X?}", &output[..])
    }

    #[test]
    fn open_connection() {
        let cont = Container::new("ce8c4a3e-96b3-11e9-9bfd-c85b7644b4a4");

        let conn = cont
            .connect(ConnectionOptions {
                host: "localhost",
                port: 5672,
            })
            .unwrap();

        println!("YAY!");
    }
}
