/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! Dove is an open source Rust implementation of the AMQP 1.0 OASIS standard (<http://www.amqp.org/>). The Advanced Message Queuing Protocol (AMQP) is an open standard for passing business messages between applications or organizations. It connects systems, feeds business processes with the information they need and reliably transmits onward the instructions that achieve their goals.
//!
//! Dove aims to be an AMQP 1.0 implementation with the following properties:
//!
//! Low footprint - efficient memory usage and pay only for what you use.
//! Portable - minimize the number of dependencies and use portable APIs.
//! The library supports only the basics right now: Establishing connections, creating sessions, links and sending and receiving message. Most AMQP 1.0 types have been implemented, and conversion for many Rust native types exists. Support for SASL ANONYMOUS and PLAIN.
//!
//! Dove exposes two different APIs:
//!
//! * An API for writing messaging applications using async rust.
//! * A low level connection API that allows you to send and receive frames defined as rust types.

//! # Example
//!
//! ```ignore
//! use dove::container::*;
//! use futures::executor::block_on;
//! use testcontainers::{clients, images, Docker};
//! env_logger::init();
//!
//! // Start a broker that we can run the client against.
//! let docker = clients::Cli::default();
//! let node = docker.run(
//!     images::generic::GenericImage::new("docker.io/vromero/activemq-artemis:2-latest")
//!         .with_env_var("ARTEMIS_USERNAME", "test")
//!         .with_env_var("ARTEMIS_PASSWORD", "test")
//! );
//! std::thread::sleep(std::time::Duration::from_millis(30000));
//! let port: u16 = node.get_host_port(5672).unwrap();
//!
//! // Create client and connect
//! let container = Container::new()
//!     .expect("unable to create container")
//!     .start();
//!
//! // connect creates the TCP connection and sends OPEN frame.
//! block_on(async {
//!     let connection = container
//!         .connect("localhost", port, ConnectionOptions::new()
//!             .sasl_mechanism(SaslMechanism::Plain)
//!             .username("test")
//!             .password("test"))
//!         .await
//!         .expect("connection not created");
//!
//!     // new_session creates the AMQP session.
//!     let session = connection
//!         .new_session(None)
//!         .await
//!         .expect("session not created");
//!
//!     // Create receiver
//!     let receiver = session
//!         .new_receiver("queue1")
//!         .await
//!         .expect("receiver not created");
//!
//!     // Create sender
//!     let sender = session
//!         .new_sender("queue1")
//!         .await
//!         .expect("sender not created");
//!
//!     //  Send message and get delivery.
//!     let message = Message::amqp_value(Value::String("Hello, World".to_string()));
//!     let _ = sender.send(message).await.expect("delivery not received");
//!
//!     // Receive message. Disposition will be sent in destructor of delivery.
//!     let delivery = receiver.receive().await.expect("unable to receive message");
//!
//!     println!("Received: {:?}", delivery.message().body);
//!
//! });
//! ```

#[macro_use]
extern crate log;

pub mod conn;
pub mod connection;
pub mod container;
pub mod convert;
pub mod decoding;
pub mod driver;
pub mod encoding;
pub mod error;
pub mod frame_codec;
pub mod framing;
pub mod message;
pub mod options;
pub mod sasl;
pub mod symbol;
pub mod transport;
pub mod types;
pub mod url;
