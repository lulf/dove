/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use dove::client::*;
use dove::conn::*;
use dove::driver::*;
use dove::framing::*;
use dove::sasl::*;
use std::env;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;

struct App {
    address: String,
    message: String,
    done: Sender<bool>,
}

/**
 * Example client that sends a single message to an AMQP endpoint.
 */
fn main() {
    /*
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

        let (out, done) = channel::<bool>();

        impl EventHandler for App {
            fn connected(&self, conn: &mut ConnectionHandle) {
                conn.open();
                let session = conn.create_session();
                session.open();
                let sender = session.create_sender(Some(self.address.as_str()));
                sender.open();
            }
            fn flow(&self, link: &mut Link) {
                link.send(self.message.as_str());
            }
            fn disposition(&self, _: &Disposition) {
                self.done.send(true).expect("Error signalling done");
            }
        };

        let client = Client::new(Box::new(App {
            address: address.to_string(),
            message: message.to_string(),
            done: out,
        }));
        client
            .connect(&host, port, opts)
            .expect("Error opening connection");

        done.recv().unwrap();
    */
}
