# Dove - Rust AMQP 1.0 Library

Dove is an open source Rust implementation of the AMQP 1.0 OASIS standard (http://www.amqp.org/). The Advanced Message Queuing Protocol (AMQP) is an open standard for passing business messages between applications or organizations.  It connects systems, feeds business processes with the information they need and reliably transmits onward the instructions that achieve their goals. 

Dove aims to be an AMQP 1.0 implementation with the following properties:

* Low footprint - minimize memory usage, pay only for what you use
* Portable - minimize the number of dependencies and use portable APIs

## Example

## TODO

* Support link layer
* Finish SASL support
* TLS/SSL support
* Some AMQP 1.0 types are not yet implemented
* A higher level API for messaging clients to improve ease of use

## Modules

* types - AMQP type system with encoding and decoding
* error - AMQP error types and error handling data types
* framing - API for frame types and encoding/decoding of frames
* transport - API for the underlying transport/network
* sasl - SASL handling
* core - Low level API for writing AMQP clients

At present, the only a low level API for sending and receiving messages is provided. This API allows you to create AMQP connections, and a driver for polling events in a non-blocking fashion. Higher level APIs (reactive, threaded etc.) can be built on top of this module.

