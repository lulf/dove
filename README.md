[![Build Status](https://travis-ci.com/lulf/dove.svg?branch=master)](https://travis-ci.com/lulf/dove)

# Dove - Rust AMQP 1.0 Library

Dove is an open source Rust implementation of the AMQP 1.0 OASIS standard (http://www.amqp.org/). The Advanced Message Queuing Protocol (AMQP) is an open standard for passing business messages between applications or organizations.  It connects systems, feeds business processes with the information they need and reliably transmits onward the instructions that achieve their goals. 

Dove aims to be an AMQP 1.0 implementation with the following properties:

* Low footprint - efficient memory usage and pay only for what you use.
* Portable - minimize the number of dependencies and use portable APIs.

The library supports only the basics right now: Establishing connections, creating sessions, links and sending and receiving message. Most AMQP 1.0 types have been implemented, and conversion for many Rust native types exists. Support for SASL ANONYMOUS and PLAIN.

Dove exposes two different APIs:

* An API for writing messaging applications using async rust.
* A low level connection API that allows you to send and receive frames defined as rust types.

## Building

```
cargo build
```

## Testing

Testing uses [testcontainers-rs](https://github.com/testcontainers/testcontainers-rs) which requires docker to run:

```
RUST_LOG=info cargo test
```

## TODO

See [TODO Issues](https://github.com/lulf/dove/issues?q=is%3Aissue+is%3Aopen+label%3Atodo)

## Examples

Client examples can be found in the [examples/](https://github.com/lulf/dove/tree/master/examples) directory. You can run them like this:

```
cargo run --example send amqp://localhost:5672/myqueue "Hello, World"
cargo run --example receive amqp://localhost:5672/myqueue
```

## Modules

* types - AMQP type system with encoding and decoding
* frame_codec - AMQP frame codec utility
* convert - Convertion of rust types and AMQP types
* encoding - AMQP type encoding
* decoding - AMQP type decoding
* error - AMQP error types and error handling data types
* framing - API for frame types and encoding/decoding of frames
* transport - API for the underlying transport/network
* message - API for working with messages
* sasl - SASL handling
* conn - Low level API for sending and recieving frames on a connection
* driver - Functionality for handling most control logic.
* container - API for writing applications
* url - utilities for handling AMQP urls
