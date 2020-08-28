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
 * Example client that uses the driver API to send and receive a message.
 */
fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 5 {
        println!("Usage: ./example_driver localhost 5672 myqueue 'Hello, world'");
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
    let mut sent = false;
    let mut event_buffer = EventBuffer::new();

    while !done {
        match driver.poll(&mut event_buffer) {
            Ok(_) => {
                for event in event_buffer.drain(..) {
                    match event {
                        Event::ConnectionInit(cid) => {
                            let conn = driver.connection(cid).unwrap();
                            conn.open();
                        }
                        Event::RemoteOpen(cid, _) => {
                            let conn = driver.connection(cid).unwrap();
                            let session = conn.create_session();
                            session.open();
                        }
                        Event::RemoteBegin(cid, chan, _) => {
                            let conn = driver.connection(cid).unwrap();
                            let session = conn.get_session(chan).unwrap();
                            let receiver = session.create_receiver(Some(address));
                            receiver.open();
                        }
                        Event::RemoteAttach(cid, chan, handle, _) => {
                            let conn = driver.connection(cid).unwrap();
                            let session = conn.get_session(chan).unwrap();
                            let link = session.get_link(handle).unwrap();
                            if link.role == LinkRole::Receiver {
                                link.flow(10);
                                let sender = session.create_sender(Some(address));
                                sender.open();
                            }
                        }
                        Event::Flow(cid, chan, handle, _) => {
                            let conn = driver.connection(cid).unwrap();
                            let session = conn.get_session(chan).unwrap();
                            let link = session.get_link(handle).unwrap();
                            if link.role == LinkRole::Sender && !sent {
                                link.send(message);
                                sent = true;
                            }
                        }
                        Event::Delivery(cid, chan, handle, delivery) => {
                            let conn = driver.connection(cid).unwrap();
                            let session = conn.get_session(chan).unwrap();
                            let link = session.get_link(handle).unwrap();
                            link.settle(&delivery, true, DeliveryState::Accepted);
                            conn.close(None);
                        }
                        Event::RemoteClose(cid, _) => {
                            let conn = driver.connection(cid).unwrap();
                            conn.close(None);
                            done = true;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                println!("Error: {:?}", e);
            }
        }
    }
}
