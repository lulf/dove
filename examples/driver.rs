/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use dove::conn::*;
use dove::driver::*;
use dove::error::*;
use dove::framing::*;
use dove::sasl::*;
use mio::{Events, Interest, Poll, Registry, Token};
use std::env;
use std::time::Duration;

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

    // For handling connection processing
    let mut poll = Poll::new().expect("error creating poller");
    let mut events = Events::with_capacity(1024);
    let mut driver = ConnectionDriver::new(1, connection);
    let id = driver.token();
    poll.registry()
        .register(&mut driver, id, Interest::READABLE | Interest::WRITABLE)
        .expect("error registering connection");

    let mut done = false;
    let mut sent = false;
    let mut event_buffer = EventBuffer::new();

    while !done {
        poll.poll(&mut events, Some(Duration::from_secs(5)))
            .expect("error during poll");
        for event in &events {
            if event.token() == id {
                // Do work until we get blocked
                while !done {
                    match driver.do_work(&mut event_buffer) {
                        Ok(_) => {
                            for event in event_buffer.drain(..) {
                                match event {
                                    Event::ConnectionInit(cid) => {
                                        driver.open();
                                    }
                                    Event::RemoteOpen(cid, _) => {
                                        let session = driver.create_session();
                                        session.open();
                                    }
                                    Event::RemoteBegin(cid, chan, _) => {
                                        let session = driver.get_session(chan).unwrap();
                                        let receiver = session.create_receiver(Some(address));
                                        receiver.open();
                                    }
                                    Event::RemoteAttach(cid, chan, handle, _) => {
                                        let session = driver.get_session(chan).unwrap();
                                        let link = session.get_link(handle).unwrap();
                                        if link.role == LinkRole::Receiver {
                                            link.flow(10);
                                            let sender = session.create_sender(Some(address));
                                            sender.open();
                                        }
                                    }
                                    Event::Flow(cid, chan, handle, _) => {
                                        let session = driver.get_session(chan).unwrap();
                                        let link = session.get_link(handle).unwrap();
                                        if link.role == LinkRole::Sender && !sent {
                                            link.send(message);
                                            sent = true;
                                        }
                                    }
                                    Event::Delivery(cid, chan, handle, delivery) => {
                                        let session = driver.get_session(chan).unwrap();
                                        let link = session.get_link(handle).unwrap();
                                        link.settle(&delivery, true, DeliveryState::Accepted);
                                        driver.close(None);
                                    }
                                    Event::RemoteClose(cid, _) => {
                                        driver.close(None);
                                        done = true;
                                    }
                                    _ => {}
                                }
                            }
                        }
                        // This means that we should poll again to await further I/O action for this driver.
                        Err(AmqpError::IoError(ref e))
                            if e.kind() == std::io::ErrorKind::WouldBlock =>
                        {
                            break;
                        }
                        Err(e) => {
                            println!("Error: {:?}", e);
                        }
                    }
                }
            }
        }
    }
}
