/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use dove::conn::*;
use dove::framing::*;
use dove::message::*;
use dove::types::*;
use dove::sasl::*;
use std::env;

/**
 * Example client that sends a single message to an AMQP endpoint with minimal dependencies.
 */
fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 5 {
        println!("Usage: ./example_send localhost 5672 myqueue 'Hello, world'");
        std::process::exit(1);
    }
    let host = &args[1];
    let port = args[2].parse::<u16>().expect("Error parsing port");
    let address = &args[3];
    let message = Message::amqp_value(Value::String(args[4].to_string()));

    let opts = ConnectionOptions::new().sasl_mechanism(SaslMechanism::Anonymous);
    let mut connection = connect(&host, port, opts).expect("Error opening connection");

    connection.open(Open::new("example-send-minimal")).unwrap();
    connection.begin(0, Begin::new(0, std::u32::MAX, std::u32::MAX)).unwrap();
    connection.attach(0, Attach {
        name: address.to_string(),
        handle: 0,
        role: LinkRole::Sender,
        target: Some(Target {
            address: Some(address.to_string()),
            durable: None,
            expiry_policy: None,
            timeout: None,
            dynamic: Some(false),
            dynamic_node_properties: None,
            capabilities: None,
        }),
        initial_delivery_count: Some(0),
        unsettled: None,
        incomplete_unsettled: None,
        snd_settle_mode: None,
        rcv_settle_mode: None,
        source: None,
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }).unwrap();

    let mut buffer = Vec::new();
    message.encode(&mut buffer).unwrap();

    loop {
        let mut received = Vec::new();
        connection.process(&mut received);
        for frame in received.drain(..) {
            println!("Process: {:?}", frame);
            match frame {
                Frame::AMQP(AmqpFrame{
                    channel,
                    performative,
                    payload
                }) => 
                match performative {
                    Some(Performative::Flow(_)) => {
                        connection.transfer(0, Transfer {
                            handle: 0,
                            delivery_id: Some(0),
                            delivery_tag: Some(vec![1]),
                            settled: Some(true),
                            aborted: None,
                            batchable: None,
                            message_format: None,
                            rcv_settle_mode: None,
                            resume: None,
                            more: None,
                            state: None,

                        }, Some(buffer.clone())).unwrap();
                        connection.close(Close{error: None}).unwrap();
                    }
                }
                }
                _ => {}
            }
        }
    }
}
