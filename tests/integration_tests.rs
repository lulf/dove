/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use std::io;
use std::thread;
use std::time;

extern crate knall;

use knall::core::*;
use knall::error::*;
use knall::*;

#[test]
fn client() {
    let mut cont = Container::new("ce8c4a3e-96b3-11e9-9bfd-c85b7644b4a4");

    let mut connection = cont
        .connect(ConnectionOptions {
            host: "localhost",
            port: 5672,
        })
        .expect("Error opening connection");

    let mut driver = ConnectionDriver::new();

    let client = driver.register(connection);

    let mut event_buffer = EventBuffer::new();

    loop {
        match driver.poll(&mut event_buffer) {
            Ok(Some(handle)) => {
                let conn = driver.connection(&handle).unwrap();
                for event in event_buffer.drain(..) {
                    match event {
                        Event::ConnectionInit => {
                            println!("Opening connection!");
                            conn.open();
                        }
                        Event::RemoteOpen(_) => {
                            println!("Remote opened!");
                            let session = conn.session();
                            session.begin();
                        }
                        Event::RemoteBegin(_, _) => {
                            println!("Remote begin");
                            /*
                            conn.close(Some(ErrorCondition {
                                condition: condition::RESOURCE_LIMIT_EXCEEDED.to_string(),
                                description: "Buhuu".to_string(),
                            }))
                            */
                        }
                        Event::RemoteClose(_) => {
                            println!("Received close from peer, closing connection!");
                            conn.close(None);
                        }
                        e => {
                            println!("Unhandled event: {:#?}", e);
                        }
                    }
                }
            }
            Ok(None) => {
                thread::sleep(time::Duration::from_millis(100));
                continue;
            }
            Err(e) => {
                println!("Got error: {:?}", e);
                assert!(false);
            }
        }
    }
}
