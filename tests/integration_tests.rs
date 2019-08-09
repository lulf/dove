/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use std::io;
use std::thread;
use std::time;

extern crate ramqp;

use ramqp::*;

#[test]
fn client() {
    let mut cont = Container::new("ce8c4a3e-96b3-11e9-9bfd-c85b7644b4a4");

    let mut connection = cont
        .connect(ConnectionOptions {
            host: "localhost",
            port: 5672,
        })
        .expect("Error opening connection");

    let mut driver = ConnectionDriver::new(connection);

    loop {
        let mut next_event = driver.next_event();
        match next_event {
            Ok(Some(event)) => match event {
                Event::ConnectionInit(mut conn) => {
                    println!("Opening connection!");
                    conn.open();
                }
                Event::RemoteClose(mut conn, _) => {
                    println!("Received close from peer, closing connection!");
                    conn.close(None);
                }
                e => {
                    println!("Unhandled event: {:#?}", e);
                }
            },
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
