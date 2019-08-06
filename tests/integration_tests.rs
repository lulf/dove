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

    let mut conn = cont
        .connect(ConnectionOptions {
            host: "localhost",
            port: 5672,
        })
        .expect("Error opening connection");

    loop {
        let next_event = conn.next_event();
        match next_event {
            Ok(Some(event)) => match event {
                Event::ConnectionInit => {
                    conn.open();
                }
                e => {
                    println!("Unhandled event: {:?}", e);
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
