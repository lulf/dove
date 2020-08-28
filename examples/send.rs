/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use dove::conn::*;
use dove::driver::*;
use dove::framing::*;
use dove::sasl::*;
use std::env;

/**
 * Example client that sends a single message to an AMQP endpoint.
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
    let message = &args[4];

    let opts = ConnectionOptions::new().sasl_mechanism(SaslMechanism::Anonymous);
    let connection = connect(&host, port, opts).expect("Error opening connection");

    // For multiplexing connections
    let mut driver = ConnectionDriver::new();
    driver.register(1, connection);

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

                            let sender = session.create_sender(Some(address));
                            sender.open();
                            sender.send(message);
                        }
                        Event::Disposition(cid, _, disposition) => {
                            if disposition.settled == Some(true)
                                && disposition.state == Some(DeliveryState::Accepted)
                            {
                                println!("Message sent!");
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
