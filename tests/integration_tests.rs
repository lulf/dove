/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

extern crate dove;

use dove::conn::*;
use dove::driver::*;
use dove::framing::*;
use dove::sasl::*;

#[test]
fn client() {
    let opts = ConnectionOptions::new().sasl_mechanism(SaslMechanism::Anonymous);
    //    let opts = ConnectionOptions::new().sasl_mechanism(SaslMechanism::Plain).username("test").password("test");

    let connection = connect("localhost", 5672, opts).expect("Error opening connection");

    let mut driver = ConnectionDriver::new();

    driver.register(2, connection);

    let mut event_buffer = EventBuffer::new();
    let mut sent = false;
    let mut done = false;

    while !done {
        match driver.poll(&mut event_buffer) {
            Ok(_) => {
                for event in event_buffer.drain(..) {
                    match event {
                        Event::ConnectionInit(cid) => {
                            println!("Opening connection!");
                            let conn = driver.connection(cid).unwrap();
                            conn.open();
                        }
                        Event::RemoteOpen(cid, _) => {
                            println!("Remote opened!");
                            let conn = driver.connection(cid).unwrap();
                            let session = conn.create_session();
                            session.open();
                        }
                        Event::RemoteBegin(cid, chan, _) => {
                            println!("Remote begin");
                            let conn = driver.connection(cid).unwrap();
                            let session = conn.get_session(chan).unwrap();
                            let receiver = session.create_receiver(Some("a"));
                            receiver.open();
                        }
                        Event::RemoteAttach(cid, chan, handle, _) => {
                            let conn = driver.connection(cid).unwrap();
                            let session = conn.get_session(chan).unwrap();
                            let link = session.get_link(handle).unwrap();
                            if link.role == LinkRole::Receiver {
                                link.flow(10);
                                let sender = session.create_sender(Some("a"));
                                sender.open();
                            }
                        }
                        Event::Flow(cid, chan, handle, flow) => {
                            println!("Received flow ({:?} credits)", flow.link_credit);
                            let conn = driver.connection(cid).unwrap();
                            let session = conn.get_session(chan).unwrap();
                            let link = session.get_link(handle).unwrap();
                            if link.role == LinkRole::Sender && !sent {
                                println!("Sending message!");
                                link.send("Hello, World");
                                sent = true;
                            }
                        }
                        Event::Delivery(cid, _, _, delivery) => {
                            println!("Received message: {:?}", delivery.message.body);
                            let conn = driver.connection(cid).unwrap();
                            // let session = conn.get_session(chan).unwrap();
                            // let link = session.get_link(handle).unwrap();
                            // link.settle(delivery, DeliveryState::Accepted);
                            conn.close(None);
                        }
                        Event::Disposition(_, _, disposition) => {
                            if let Some(settled) = disposition.settled {
                                if let Some(state) = disposition.state {
                                    if settled && state == DeliveryState::Accepted {
                                        println!("Message delivered!");
                                    } else {
                                        println!("Error delivering message!");
                                    }
                                }
                            }
                        }
                        Event::RemoteClose(cid, close) => {
                            let conn = driver.connection(cid).unwrap();
                            println!(
                                "Received close from peer ({:?}), closing connection!",
                                close
                            );
                            conn.close(None);
                            done = true;
                        }
                        e => {
                            println!("Unhandled event: {:#?}", e);
                        }
                    }
                }
            }
            Err(e) => {
                println!("Got error: {:?}", e);
                assert!(false);
            }
        }
    }
}

//#[test]
/*
fn server() {
    let mut listener = listen(
        "localhost",
        5672,
        ListenOptions {
            container_id: "ce8c4a3e-96b3-11e9-9bfd-c85b7644b4a4",
        },
    )
    .expect("Error creating listener");

    let mut driver = ConnectionDriver::new();

    let connection = listener.accept().unwrap();

    driver.register(connection);

    let mut event_buffer = EventBuffer::new();
    loop {
        match driver.poll(&mut event_buffer) {
            Ok(_) => {
                for event in event_buffer.drain(..) {
                    match event {
                        Event::ConnectionInit(_) => {}
                        Event::RemoteOpen(cid, _) => {
                            println!("Remote opened!");
                            let conn = driver.connection(cid).unwrap();
                            conn.open();
                        }
                        Event::RemoteBegin(cid, chan, _) => {
                            println!("Remote begin");
                            let conn = driver.connection(cid).unwrap();

                            let session = conn.get_session(chan).unwrap();
                            session.open();
                        }
                        Event::RemoteClose(cid, _) => {
                            println!("Received close from peer, closing connection!");
                            let conn = driver.connection(cid).unwrap();
                            conn.close(None);
                        }
                        e => {
                            println!("Unhandled event: {:#?}", e);
                        }
                    }
                }
            }
            Err(e) => {
                println!("Got error: {:?}", e);
                assert!(false);
            }
        }
    }
}
*/
