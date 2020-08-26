/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use dove::core::*;
use dove::message::*;
use dove::sasl::*;
use dove::types::*;
use std::env;

/**
 * Example client that receives a single message from an AMQP endpoint.
 */
fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 4 {
        println!("Usage: ./example_receive localhost 5672 myqueue");
        std::process::exit(1);
    }
    let host = &args[1];
    let port = args[2].parse::<u16>().expect("Error parsing port");
    let address = &args[3];

    let mut opts = ConnectionOptions::new("example-receive");
    opts.sasl_mechanism = Some(SaslMechanism::Anonymous);

    let connection = connect(1, &host, port, opts).expect("Error opening connection");

    // For multiplexing connections
    let mut driver = ConnectionDriver::new();
    driver.register(connection);

    let mut done = false;

    let mut event_buffer = EventBuffer::new();
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

                            let receiver = session.create_receiver(Some(address));
                            receiver.open();
                            receiver.flow(1);
                        }
                        Event::Delivery(cid, _, _, delivery) => {
                            if let MessageBody::AmqpValue(Value::String(s)) = delivery.message.body
                            {
                                println!("Received message: {:?}", s);
                                let conn = driver.connection(cid).unwrap();
                                conn.close(None);
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
