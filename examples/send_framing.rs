/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use dove::conn::*;
use dove::framing::*;
use dove::message::*;
use dove::sasl::*;
use dove::types::*;
use std::env;

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

    let opts = ConnectionOptions::new().sasl_mechanism(SaslMechanism::Anonymous);
    let mut connection = connect(&host, port, opts).expect("Error opening connection");

    connection.open(Open::new("example-send-minimal")).unwrap();
    connection
        .begin(0, Begin::new(0, std::u32::MAX, std::u32::MAX))
        .unwrap();
    connection
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
                                connection
                                    .transfer(
                                        0,
                                        Transfer::new(0)
                                            .delivery_id(0)
                                            .delivery_tag(&[1])
                                            .settled(true),
                                        Some(buffer.clone()),
                                    )
                                    .unwrap();
                                connection.close(Close { error: None }).unwrap();
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
