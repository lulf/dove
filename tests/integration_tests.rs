/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

extern crate dove;

use dove::core::*;
use dove::sasl::*;

#[test]
fn client() {
    let mut opts = ConnectionOptions::new("ce8c4a3e-96b3-11e9-9bfd-c85b7644b4a4");
    //    opts.username = Some("test".to_string());
    //    opts.password = Some("test".to_string());
    opts.sasl_mechanism = Some(SaslMechanism::Anonymous);

    println!("REGISTERING");
    let connection = connect(2, "localhost", 5672, opts).expect("Error opening connection");

    let mut driver = ConnectionDriver::new();

    driver.register(connection);

    let mut event_buffer = EventBuffer::new();

    loop {
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
                            let sender = session.create_sender(Some("a"));
                            sender.open();
                            /*
                            conn.close(Some(ErrorCondition {
                                condition: condition::RESOURCE_LIMIT_EXCEEDED.to_string(),
                                description: "Buhuu".to_string(),
                            }))
                            */
                        }
                        Event::RemoteClose(cid, close) => {
                            let conn = driver.connection(cid).unwrap();
                            println!(
                                "Received close from peer ({:?}), closing connection!",
                                close
                            );
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
