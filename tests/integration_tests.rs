/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use std::io;
use std::thread;
use std::time;

extern crate rust_amqp;

use rust_amqp::*;

#[test]
fn client() {
    let mut cont = Container::new("ce8c4a3e-96b3-11e9-9bfd-c85b7644b4a4");

    let mut conn = cont
        .connect(ConnectionOptions {
            host: "localhost",
            port: 5672,
        })
        .expect("Error opening connection");

    loop {
        let event = conn.next_event();
        match event {
            Ok(Some(Event::ConnectionInit)) => {
                println!("Got initialization event");
                conn.open();
            }
            Ok(e) => println!("Got event: {:?}", e),
            Err(AmqpError::IoError(ref e)) if e.kind() == io::ErrorKind::WouldBlock => {
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
