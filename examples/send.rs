/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use dove::container::*;
use futures::executor::block_on;
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
    let data = &args[4];

    let opts = ConnectionOptions::new().sasl_mechanism(SaslMechanism::Anonymous);

    let container = Container::new()
        .expect("unable to create container")
        .start();

    // connect creates the TCP connection and sends OPEN frame.
    block_on(async {
        let connection = container
            .connect(&host, port, opts)
            .await
            .expect("connection not created");

        // new_session creates the AMQP session.
        let session = connection
            .new_session(None)
            .await
            .expect("session not created");

        // new_sender creates the AMQP sender link.
        let sender = session
            .new_sender(address)
            .await
            .expect("sender not created");

        //  Send message and get delivery.
        let message = Message::amqp_value(Value::String(data.to_string()));
        let _ = sender.send(message).await.expect("delivery not received");
    });
}
