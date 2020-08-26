# Dove - Rust AMQP 1.0 Library

Dove is an open source Rust implementation of the AMQP 1.0 OASIS standard (http://www.amqp.org/). The Advanced Message Queuing Protocol (AMQP) is an open standard for passing business messages between applications or organizations.  It connects systems, feeds business processes with the information they need and reliably transmits onward the instructions that achieve their goals. 

Dove aims to be an AMQP 1.0 implementation with the following properties:

* Low footprint - minimize memory usage, pay only for what you use
* Portable - minimize the number of dependencies and use portable APIs

At present, the library supports basic support for establishing connections, creating sessions, links and sending and receiving messages. Most AMQP 1.0 types have been implemented, and conversion for many Rust native types exists.

## Core API example - sending a message

```
use dove::core::*;

fn main() {
    let mut opts = ConnectionOptions::new("ce8c4a3e-96b3-11e9-9bfd-c85b7644b4a4");
    opts.sasl_mechanism = Some(SaslMechanism::Anonymous);

    let connection = connect(2, "localhost", 5672, opts).expect("Error opening connection");

    // For multiplexing connections
    let mut driver = ConnectionDriver::new();
    driver.register(connection);

    let mut event_buffer = EventBuffer::new();
    let mut done = false;

    while !done {
        match driver.poll(&mut event_buffer) {
            Ok(_) => {
                for event in event_buffer.drain(..) {
                    match event {
                        Event::ConnectionInit(cid) => {
                            let conn = driver.connection(cid).unwrap();
                            conn.open();

                            let session = conn.create_session();
                            session.open();

                            let sender = session.create_sender(Some("myqueue"));
                            sender.open();
                            sender.send("Hello, World");
                        }
                        Event::Disposition(cid, _, disposition) => {
                            if disposition.settled == Some(true)
                                && disposition.state == Some(DeliveryState::Accepted)
                            {
                                driver.connection(cid).unwrap().close(None);
                            }
                        }
                        Event::RemoteClose(_, _) => {
                            done = true;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                println!("Got error: {:?}", e);
            }
        }
    }
}
```


## TODO

* Finish SASL support (missing SCRAM* support)
* TLS/SSL support
* Some AMQP 1.0 types are not yet implemented
* A higher level API for messaging clients to improve ease of use

## Modules

* types - AMQP type system with encoding and decoding
* frame_codec - AMQP frame codec utility
* convert - Convertion of rust types and AMQP types
* encoding - AMQP type encoding
* decoding - AMQP type decoding
* error - AMQP error types and error handling data types
* framing - API for frame types and encoding/decoding of frames
* transport - API for the underlying transport/network
* sasl - SASL handling
* core - Low level API for writing AMQP clients

At present, the only a low level API for sending and receiving messages is provided. This API allows you to create AMQP connections, and a driver for polling events in a non-blocking fashion. Higher level APIs (reactive, threaded etc.) can be built on top of this module.

