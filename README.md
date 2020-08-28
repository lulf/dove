# Dove - Rust AMQP 1.0 Library

Dove is an open source Rust implementation of the AMQP 1.0 OASIS standard (http://www.amqp.org/). The Advanced Message Queuing Protocol (AMQP) is an open standard for passing business messages between applications or organizations.  It connects systems, feeds business processes with the information they need and reliably transmits onward the instructions that achieve their goals. 

Dove aims to be an AMQP 1.0 implementation with the following properties:

* Low footprint - efficient memory usage and pay only for what you use.
* Portable - minimize the number of dependencies and use portable APIs.

The library supports only the basics right now: Establishing connections, creating sessions, links and sending and receiving message. Most AMQP 1.0 types have been implemented, and conversion for many Rust native types exists. Support for SASL ANONYMOUS and PLAIN.

Dove exposes three different APIs:

* A connection API that allows you to send and receive frames defined as rust types. This could be
  useful for embedded systems.
* A driver API that enables multiplexing of multiple connections and processing events for those connections.
* A client reactor-like API with an event handler.

The goal is to create a higher level API based on this to make it easier to write AMQP clients.

## TODO

* Cleaning up APIs (which parts should be private/public).
* Cleaning up crate/module structure.
* Experiment with Rust async
* TLS/SSL support
* Improve SASL support (missing SCRAM* support)
* Complete implementation of encoding+decoding for all AMQP 1.0 types.
* Improve test coverage.
* A higher level API for messaging clients to improve ease of use
* Compile to WASM.

## Examples

Client examples can be found in the [examples/](https://github.com/lulf/dove/tree/master/examples) directory.

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
* conn - Low level API for sending and recieving frames on a connection
* driver - Low level API for multiplexing events from multiple connections
