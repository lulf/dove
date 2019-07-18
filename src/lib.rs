/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use byteorder::NetworkEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use std::collections::BTreeMap;
use std::convert::From;
use std::error;
use std::fmt;
use std::io;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::vec::Vec;
use uuid::Uuid;

type Result<T> = std::result::Result<T, AmqpError>;

enum Kind {
    Null,
    Ushort,
    Uint,
    Ulong,
    String,
    List,
    Map,
}

#[derive(Clone, PartialEq, Debug, PartialOrd)]
enum Value {
    Null,
    Ushort(u16),
    Uint(u32),
    Ulong(u64),
    String(String),
    List(Vec<Value>),
    Map(BTreeMap<Value, Value>),
}

impl Value {
    fn to_string(self: &Self) -> String {
        match self {
            Value::String(v) => v.clone(),
            _ => panic!("Unexpected type"),
        }
    }

    fn to_u32(self: &Self) -> u32 {
        match self {
            Value::Ushort(v) => (*v as u32),
            Value::Uint(v) => (*v as u32),
            _ => panic!("Unexpected type"),
        }
    }

    fn to_u16(self: &Self) -> u16 {
        match self {
            Value::Ushort(v) => (*v as u16),
            _ => panic!("Unexpected type"),
        }
    }
}

const U8_MAX: usize = std::u8::MAX as usize;
const U32_MAX: usize = std::u32::MAX as usize;
const LIST8_MAX: usize = (std::u8::MAX as usize) - 1;
const LIST32_MAX: usize = (std::u32::MAX as usize) - 4;

fn encode_ref(value: &Value, stream: &mut Write) -> Result<usize> {
    match value {
        Value::Null => {
            stream.write_u8(TypeCode::Null as u8)?;
            Ok(1)
        }
        Value::String(val) => {
            if val.len() > U8_MAX {
                stream.write_u8(TypeCode::Str32 as u8)?;
                stream.write_u32::<NetworkEndian>(val.len() as u32)?;
                stream.write(val.as_bytes())?;
                Ok(5 + val.len())
            } else {
                stream.write_u8(TypeCode::Str8 as u8)?;
                stream.write_u8(val.len() as u8)?;
                stream.write(val.as_bytes())?;
                Ok(2 + val.len())
            }
        }
        Value::Ushort(val) => {
            stream.write_u8(TypeCode::Ushort as u8)?;
            stream.write_u16::<NetworkEndian>(*val)?;
            Ok(3)
        }
        Value::Uint(val) => {
            if *val > U8_MAX as u32 {
                stream.write_u8(TypeCode::Uint as u8)?;
                stream.write_u32::<NetworkEndian>(*val)?;
                Ok(5)
            } else if *val > 0 {
                stream.write_u8(TypeCode::Uintsmall as u8)?;
                stream.write_u8(*val as u8)?;
                Ok(2)
            } else {
                stream.write_u8(TypeCode::Uint0 as u8)?;
                Ok(1)
            }
        }
        Value::Ulong(val) => {
            if *val > U8_MAX as u64 {
                stream.write_u8(TypeCode::Ulong as u8)?;
                stream.write_u64::<NetworkEndian>(*val)?;
                Ok(9)
            } else if *val > 0 {
                stream.write_u8(TypeCode::Ulongsmall as u8)?;
                stream.write_u8(*val as u8)?;
                Ok(2)
            } else {
                stream.write_u8(TypeCode::Ulong0 as u8)?;
                Ok(1)
            }
        }
        Value::List(vec) => {
            let mut listbuf = Vec::new();
            for v in vec.iter() {
                encode_ref(v, &mut listbuf)?;
            }

            if listbuf.len() > LIST32_MAX {
                Err(AmqpError::new(
                    "Encoded list size cannot be longer than 4294967291 bytes",
                ))
            } else if listbuf.len() > LIST8_MAX {
                stream.write_u8(TypeCode::List32 as u8)?;
                stream.write_u32::<NetworkEndian>((4 + listbuf.len()) as u32)?;
                stream.write_u32::<NetworkEndian>(vec.len() as u32)?;
                stream.write(&listbuf[..]);
                Ok(9 + listbuf.len())
            } else if listbuf.len() > 0 {
                stream.write_u8(TypeCode::List8 as u8)?;
                stream.write_u8((1 + listbuf.len()) as u8)?;
                stream.write_u8(vec.len() as u8)?;
                stream.write(&listbuf[..]);
                Ok(3 + listbuf.len())
            } else {
                stream.write_u8(TypeCode::List0 as u8)?;
                Ok(1)
            }
        }
        Value::Map(m) => {
            let mut listbuf = Vec::new();
            for (key, value) in m {
                encode_ref(key, &mut listbuf)?;
                encode_ref(value, &mut listbuf)?;
            }

            let n_items = m.len() * 2;

            if listbuf.len() > LIST32_MAX {
                Err(AmqpError::new(
                    "Encoded map size cannot be longer than 4294967291 bytes",
                ))
            } else if listbuf.len() > LIST8_MAX || n_items > U8_MAX {
                stream.write_u8(TypeCode::Map32 as u8)?;
                stream.write_u32::<NetworkEndian>((4 + listbuf.len()) as u32)?;
                stream.write_u32::<NetworkEndian>(n_items as u32)?;
                stream.write(&listbuf[..]);
                Ok(9 + listbuf.len())
            } else {
                stream.write_u8(TypeCode::Map8 as u8)?;
                stream.write_u8((1 + listbuf.len()) as u8)?;
                stream.write_u8(n_items as u8)?;
                stream.write(&listbuf[..]);
                Ok(3 + listbuf.len())
            }
        }
    }
}

fn decode(stream: &mut Read) -> Result<Value> {
    let raw_code: u8 = stream.read_u8()?;
    if raw_code == 0 {
        let descriptor = decode(stream)?;
        Ok(descriptor)
    } else {
        let code = decode_type(raw_code)?;
        match code {
            TypeCode::Null => Ok(Value::Null),
            TypeCode::Ushort => {
                let val = stream.read_u16::<NetworkEndian>()?;
                Ok(Value::Ushort(val))
            }
            TypeCode::Uint => {
                let val = stream.read_u32::<NetworkEndian>()?;
                Ok(Value::Uint(val))
            }
            TypeCode::Uintsmall => {
                let val = stream.read_u8()? as u32;
                Ok(Value::Uint(val))
            }
            TypeCode::Uint0 => Ok(Value::Uint(0)),
            TypeCode::Ulong => {
                let val = stream.read_u64::<NetworkEndian>()?;
                Ok(Value::Ulong(val))
            }
            TypeCode::Ulongsmall => {
                let val = stream.read_u8()? as u64;
                Ok(Value::Ulong(val))
            }
            TypeCode::Ulong0 => Ok(Value::Ulong(0)),
            TypeCode::Str8 => {
                let len = stream.read_u8()? as usize;
                let mut buffer = vec![0u8; len];
                stream.read_exact(&mut buffer)?;
                let s = String::from_utf8(buffer)?;
                Ok(Value::String(s))
            }
            TypeCode::Str32 => {
                let len = stream.read_u32::<NetworkEndian>()? as usize;
                let mut buffer = vec![0u8; len];
                stream.read_exact(&mut buffer)?;
                let s = String::from_utf8(buffer)?;
                Ok(Value::String(s))
            }
            TypeCode::List0 => Ok(Value::List(Vec::new())),
            TypeCode::List8 => {
                let _sz = stream.read_u8()? as usize;
                let count = stream.read_u8()? as usize;
                let mut data: Vec<Value> = Vec::new();
                for _num in 0..count {
                    let result = decode(stream)?;
                    data.push(result);
                }
                Ok(Value::List(data))
            }
            TypeCode::List32 => {
                let _sz = stream.read_u32::<NetworkEndian>()? as usize;
                let count = stream.read_u32::<NetworkEndian>()? as usize;
                let mut data: Vec<Value> = Vec::new();
                for _num in 0..count {
                    let result = decode(stream)?;
                    data.push(result);
                }
                Ok(Value::List(data))
            }
            TypeCode::Map8 => {
                /*
                let sz = stream.read_u8()? as usize;
                let count = stream.read_u8()? as usize / 2;
                let mut data: BTreeMap<Value, Value> = BTreeMap::new();
                for num in (0..count) {
                    let key = decode(stream)?;
                    let value = decode(stream)?;
                    data.insert(key, value);
                }
                Ok(Value::Map(data))
                */
                Ok(Value::Null)
            }
            TypeCode::Map32 => {
                /*
                let sz = stream.read_u32::<NetworkEndian>()? as usize;
                let count = stream.read_u32::<NetworkEndian>()? as usize / 2;
                let mut data: BTreeMap<Value, Value> = BTreeMap::new();
                for num in (0..count) {
                    let key = decode(stream)?;
                    let value = decode(stream)?;
                    data.insert(key, value);
                }
                Ok(Value::Map(data))
                    */
                Ok(Value::Null)
            }
        }
    }
}

#[repr(u8)]
enum TypeCode {
    Null = 0x40,
    Ushort = 0x60,
    Uint = 0x70,
    Uintsmall = 0x52,
    Uint0 = 0x43,
    Ulong = 0x80,
    Ulongsmall = 0x53,
    Ulong0 = 0x44,
    Str8 = 0xA1,
    Str32 = 0xB1,
    List0 = 0x45,
    List8 = 0xC0,
    List32 = 0xD0,
    Map8 = 0xC1,
    Map32 = 0xD1,
}

fn decode_type(code: u8) -> Result<TypeCode> {
    match code {
        0x40 => Ok(TypeCode::Null),
        0x60 => Ok(TypeCode::Ushort),
        0x70 => Ok(TypeCode::Uint),
        0x52 => Ok(TypeCode::Uintsmall),
        0x43 => Ok(TypeCode::Uint0),
        0x80 => Ok(TypeCode::Ulong),
        0x53 => Ok(TypeCode::Ulongsmall),
        0x44 => Ok(TypeCode::Ulong0),
        0xA1 => Ok(TypeCode::Str8),
        0xB1 => Ok(TypeCode::Str32),
        0x45 => Ok(TypeCode::List0),
        0xC0 => Ok(TypeCode::List8),
        0xD0 => Ok(TypeCode::List32),
        0xC1 => Ok(TypeCode::Map8),
        0xD1 => Ok(TypeCode::Map32),
        _ => Err(AmqpError::new("Unknown code!")),
    }
}

// const TYPE_CODE_Bool = 0x56,
// const TYPE_CODE_Truebool = 0x41,
// const TYPE_CODE_Falsebool = 0x42,
// const TYPE_CODE_Ubyte = 0x50,
// const TYPE_CODE_Ushort = 0x60,
// const TYPE_CODE_Uint = 0x70,
// const TYPE_CODE_Smalluint = 0x52,
// const TYPE_CODE_Uint0 = 0x43,

// const TYPE_CODE_Byte = 0x51,
// const TYPE_CODE_Short = 0x61,
// const TYPE_CODE_Smallint = 0x54,
// const TYPE_CODE_Int = 0x71,
// const TYPE_CODE_Smalllong = 0x55,
// const TYPE_CODE_Long = 0x81,
// const TYPE_CODE_Float = 0x72,
// const TYPE_CODE_Double = 0x82,
// const TYPE_CODE_Decimal32 = 0x74,
// const TYPE_CODE_Decimal64 = 0x84,
// const TYPE_CODE_Decimal128 = 0x94,
// const TYPE_CODE_Char = 0x73,
// const TYPE_CODE_Timestamp = 0x83,
// const TYPE_CODE_Uuid = 0x98,
// const TYPE_CODE_Vbin8 = 0xA0,
// const TYPE_CODE_Vbin32 = 0xB0,
// const TYPE_CODE_Str8 = 0xA1,
// const TYPE_CODE_Str32 = 0xB1,
// const TYPE_CODE_Sym8 = 0xA3,
// const TYPE_CODE_Sym32 = 0xB3,
// const TYPE_CODE_List0 = 0x45,
// const TYPE_CODE_List8 = 0xC0,
// const TYPE_CODE_List32 = 0xD0,
// const TYPE_CODE_Map8 = 0xC1,
// const TYPE_CODE_Map32 = 0xD1,
//const TYPE_CODE_Array8 = 0xE0,
//const TYPE_CODE_Array32 = 0xF0,

/*
fn decode_value<T: AmqpCodec<T>>(stream: &mut Read) -> Result<T> {
    let code = stream.read_u8()?;
    return T::decode(code, stream);
}
*/

/*
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
*/

struct Open {
    container_id: String,
    hostname: String,
    max_frame_size: u32,
    channel_max: u16,
}

impl Default for Open {
    fn default() -> Open {
        Open {
            container_id: Uuid::new_v4().to_string(),
            hostname: String::new(),
            max_frame_size: 4294967295,
            channel_max: 65535,
        }
    }
}

enum Performative {
    Open(Open),
}

enum FrameType {
    AMQP,
    SASL,
}

struct Frame {
    frameType: FrameType,
    channel: u16,
    performative: Option<Performative>,
    payload: Option<Box<Vec<u8>>>,
}

fn encode_frame(frame: &Frame, stream: &mut Write) -> Result<usize> {
    let mut buf: Vec<u8> = Vec::new();
    let doff = 2;
    let mut sz = 8;

    buf.write_u8(doff)?;
    buf.write_u8(0)?;
    buf.write_u16::<NetworkEndian>(frame.channel)?;

    if let Some(performative) = &frame.performative {
        match performative {
            Performative::Open(open) => {
                buf.write_u8(0)?;
                sz += encode_ref(&Value::Ulong(0x10), &mut buf)? + 1;
                let args = vec![
                    Value::String(open.container_id.clone()),
                    Value::String(open.hostname.clone()),
                    Value::Uint(open.max_frame_size),
                    Value::Ushort(open.channel_max),
                ];
                sz += encode_ref(&Value::List(args), &mut buf)?;
            }
        }
    }

    if let Some(payload) = &frame.payload {
        sz += buf.write(payload)?;
    }

    stream.write_u32::<NetworkEndian>(sz as u32);
    stream.write(&buf[..]);

    Ok(sz)
}

fn decode_frame(stream: &mut Read) -> Result<Frame> {
    let sz = stream.read_u32::<NetworkEndian>()? as usize;
    let mut doff = stream.read_u8()?;
    let frame_type = stream.read_u8()?;
    if frame_type == 0 {
        let channel = stream.read_u16::<NetworkEndian>()?;
        let mut buf: Vec<u8> = Vec::new();
        while doff > 2 {
            stream.read_u32::<NetworkEndian>()?;
            doff -= 1;
        }
        let descriptor = decode(stream)?;
        let performative = match descriptor {
            Value::Ulong(0x10) => {
                let list = decode(stream)?;
                if let Value::List(args) = list {
                    let mut open = Open {
                        hostname: String::from("localhost"), // TODO: Set to my hostname if not found
                        ..Default::default()
                    };
                    let mut it = args.iter();
                    if let Some(container_id) = it.next() {
                        open.container_id = container_id.to_string();
                    }

                    if let Some(hostname) = it.next() {
                        open.hostname = hostname.to_string();
                    }

                    if let Some(max_frame_size) = it.next() {
                        open.max_frame_size = max_frame_size.to_u32();
                    }

                    if let Some(channel_max) = it.next() {
                        open.channel_max = channel_max.to_u16();
                    }

                    Ok(Performative::Open(open))
                } else {
                    Err(AmqpError::new(
                        "Missing expected arguments for open performative",
                    ))
                }
            }
            _ => Err(AmqpError::new("Unexpected descriptor value")),
        }?;
        Ok(Frame {
            channel: channel,
            frameType: FrameType::AMQP,
            performative: Some(performative),
            payload: None,
        })
    } else {
        // TODO: Print which type
        Err(AmqpError::new("Unknown frame type"))
    }
}

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

impl std::convert::From<std::string::FromUtf8Error> for AmqpError {
    fn from(error: std::string::FromUtf8Error) -> Self {
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
    //    transport: &'a Transport,
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

/*
struct Transport {
    connections: Vec<Connection>,
    thread_pool:
}

static global_transport: Transport = Transport {
    connections: Vec::new(),
};
 */

impl Container {
    pub fn new(id: &'static str) -> Container {
        Container {
            id: id,
            //         transport: &global_transport,
        }
    }

    pub fn connect(&self, opts: ConnectionOptions) -> Result<Connection> {
        let mut stream = TcpStream::connect(format!("{}:{}", opts.host, opts.port))?;
        // TODO: SASL support

        stream.write(&AMQP_10_VERSION)?;

        // AMQP OPEN
        let frame = Frame {
            frameType: FrameType::AMQP,
            channel: 0,
            performative: Some(Performative::Open(Open {
                hostname: String::from(opts.host),
                ..Default::default()
            })),
            payload: None,
        };

        encode_frame(&frame, &mut stream)?;

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
    // Wait for next connection state event
    //    pub fn wait() -> Result<Event> {}

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

    fn assert_type(value: &Value, expected_len: usize) {
        let mut output: Vec<u8> = Vec::new();
        let len = encode_ref(value, &mut output).unwrap();
        assert_eq!(expected_len, len);
        assert_eq!(expected_len, output.len());

        let decoded = decode(&mut &output[..]).unwrap();
        assert_eq!(&decoded, value);
    }

    #[test]
    fn check_types() {
        assert_type(&Value::Ulong(123), 2);
        assert_type(&Value::Ulong(1234), 9);
        assert_type(&Value::String(String::from("Hello, world")), 14);
        assert_type(&Value::String(String::from("aaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbcccccccccccccccccccccccdddddddddddddddddddddddddeeeeeeeeeeeeeeeeeeeeeeeeeffffffffffffffffffffgggggggggggggggggggggggghhhhhhhhhhhhhhhhhhhhhhhiiiiiiiiiiiiiiiiiiiiiiiijjjjjjjjjjjjjjjjjjjkkkkkkkkkkkkkkkkkkkkkkllllllllllllllllllllmmmmmmmmmmmmmmmmmmmmnnnnnnnnnnnnnnnnnnnnooooooooooooooooooooppppppppppppppppppqqqqqqqqqqqqqqqq")), 370);
        assert_type(
            &Value::List(vec![
                Value::Ulong(1),
                Value::Ulong(42),
                Value::String(String::from("Hello, world")),
            ]),
            21,
        );
    }

    #[test]
    fn check_performatives() {
        let frm = Open {
            hostname: String::from("localhost"),
            ..Default::default()
        };

        assert_eq!(String::from("localhost"), frm.hostname);
        assert_eq!(36, frm.container_id.len());
        assert_eq!(4294967295, frm.max_frame_size);
        assert_eq!(65535, frm.channel_max);
    }

    /*
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
    */

    #[test]
    fn open_connection() {
        let cont = Container::new("ce8c4a3e-96b3-11e9-9bfd-c85b7644b4a4");

        let conn = cont
            .connect(ConnectionOptions {
                host: "localhost",
                port: 5672,
            })
            .unwrap();

        //        while (conn.process()) {}

        println!("YAY!");
    }
}
