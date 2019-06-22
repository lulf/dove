/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

// use byteorder::{ByteOrder, NetworkEndian};

use std::io::Write;
use std::error;
use std::net::TcpStream;
use std::collections::HashMap;
use std::vec::Vec;
use std::fmt;

type Result<T> = std::result::Result<T, AmqpError>;

mod types {
    pub struct Null;
    pub struct Boolean(pub bool);
    pub struct Ubyte(pub u8);
    pub struct Ushort(pub u16);
    pub struct Uint(pub u32);
    pub struct Ulong(pub u64);
    pub struct Byte(pub i8);
    pub struct Short(pub i16);
    pub struct Int(pub i32);
    pub struct Long(pub i64);
    pub struct Float(pub f32);
    pub struct Double(pub f64);
    pub struct Decimal32(pub u32);
    pub struct Decimal64(pub u64);
    pub struct Decimal128(pub [u64; 2]);
    pub struct Char(pub char);
    pub struct Timestamp(pub i64);
    pub struct Uuid(pub [u8; 16]);
    pub struct Binary(pub u32);
    pub struct String(pub std::string::String);

    #[derive(Eq, PartialEq, Hash)]
    pub struct Symbol(pub &'static str);

    pub struct List<T>(pub Vec<T>);
    pub struct Map<K, V>(pub std::collections::HashMap<K, V>);
    pub struct Array<T>(pub Vec<T>);

    pub type Milliseconds = Uint;
    pub type IetfLanguageTag = Symbol;
    pub type Fields = Map<Symbol, Box<std::any::Any>>;
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


trait Frame {
    fn get_descriptor(&self) -> Descriptor;
    fn encode(&self, stream: &Write) -> Result<()>;
}

struct OpenFrame {
    container_id: types::String,
    hostname: types::String,
    max_frame_size: types::Uint,
    channel_max: types::Ushort,
    idle_time_out: types::Milliseconds,
    outgoing_locales: types::Array<types::IetfLanguageTag>,
    incoming_locales: types::Array<types::IetfLanguageTag>,
    offered_capabilities: types::Array<types::Symbol>,
    desired_capabilities: types::Array<types::Symbol>,
    properties: types::Fields
}

impl Frame for OpenFrame {
    fn get_descriptor(&self) -> Descriptor {
        return Descriptor("amqp:open:list", 0x00000010);
    }

    fn encode(&self, stream: &Write) -> Result<()> {
        return Ok(());
    }
}

impl Frame {
    fn encode(frame: &Frame, stream: &Write) -> Result<()> {
        return frame.encode(stream);
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
    id: &'static str
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
    End
}

pub struct Connection {
    stream: TcpStream,
    state: ConnectionState
}

pub struct Session {
    
}

pub struct Link {
    
}

pub struct Sender {
    
}

pub struct Receiver {
    
}

const AMQP_10_VERSION: [u8; 8] = [65, 77, 81, 80, 0, 1, 0, 0];

impl Container {

    pub fn new(id: &'static str) -> Container {
        Container {
            id: id,
        }
    }

    pub fn connect(&self, opts: ConnectionOptions) -> std::io::Result<Connection> {
        let mut stream = TcpStream::connect(format!("{}:{}", opts.host, opts.port))?;
        // TODO: SASL support

        stream.write(&AMQP_10_VERSION);

        // AMQP OPEN
        let frame = OpenFrame{
            container_id: types::String(String::from(self.id)),
            hostname: types::String(String::from(opts.host)),
            max_frame_size: types::Uint(4294967295),
            channel_max: types::Ushort(65535),
            idle_time_out: types::Uint(10000),
            outgoing_locales: types::Array(Vec::new()),
            incoming_locales: types::Array(Vec::new()),
            offered_capabilities: types::Array(Vec::new()),
            desired_capabilities: types::Array(Vec::new()),
            properties: types::Map(HashMap::new())
        };

        frame.encode(&stream);

        return Ok(Connection{stream: stream, state: ConnectionState::OpenPipe});
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

        let conn = cont.connect(ConnectionOptions{host: "127.0.0.1", port:5672}).unwrap();

        println!("YAY!");

    }
}
