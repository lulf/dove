/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use ::mio::Poll;
use ::mio::Token;
use ::mio::Waker;
use dove::conn::*;
use dove::framing::*;
use dove::message::*;
use dove::sasl::*;
use dove::transport::*;
use dove::types::*;
use std::env;
use std::sync::Arc;
use std::time::Duration;

/**
 * Example client that sends a single message to an AMQP endpoint with minimal dependencies and sending
 * and receiving frames directly on a connection.
 */
fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 5 {
        println!("Usage: ./example_send_framing localhost 5672 myqueue 'Hello, world'");
        std::process::exit(1);
    }
    let host = &args[1];
    let port = args[2].parse::<u16>().expect("Error parsing port");
    let address = &args[3];
    let message = Message::amqp_value(Value::String(args[4].to_string()));

    let opts = ConnectionOptions::new()
        .sasl_mechanism(SaslMechanism::Anonymous)
        .idle_timeout(Duration::from_secs(5));
    let net =
        mio::MioNetwork::connect(&(host.to_string(), port)).expect("Error opening network");
    let transport = Transport::new(net, 1024);
    let mut connection = connect(transport, opts).expect("Error opening connection");

    let p = Poll::new().expect("Failed to create poll");
    let waker = Arc::new(
        Waker::new(p.registry(), Token(u32::MAX as usize)).expect("Failed to create waker"),
    );
    let handle = connection.handle(waker);

    handle.open(Open::new("example-send-minimal")).unwrap();
    handle
        .begin(0, Begin::new(0, std::u32::MAX, std::u32::MAX))
        .unwrap();
    handle
        .attach(
            0,
            Attach::new(address, 0, LinkRole::Sender)
                .target(Target::new().address(address))
                .initial_delivery_count(0),
        )
        .unwrap();

    let mut buffer = Vec::new();
    message.encode(&mut buffer).unwrap();

    let mut done = false;
    while !done {
        let mut received = Vec::new();
        let result = connection.process(&mut received);
        match result {
            Ok(_) => {
                for frame in received.drain(..) {
                    match frame {
                        Frame::AMQP(AmqpFrame {
                            channel: _,
                            performative,
                            payload: _,
                        }) => match performative {
                            Some(Performative::Flow(_)) => {
                                handle
                                    .transfer(
                                        0,
                                        Transfer::new(0)
                                            .delivery_id(0)
                                            .delivery_tag(&[1])
                                            .settled(true),
                                        Some(buffer.clone()),
                                    )
                                    .unwrap();
                                handle.close(Close { error: None }).unwrap();
                            }
                            Some(Performative::Close(_)) => {
                                done = true;
                            }
                            _ => {}
                        },

                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
}
