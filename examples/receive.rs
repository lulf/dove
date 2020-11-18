/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use dove::container::*;
use futures::executor::block_on;
use std::env;

/**
 * Example client that receives a single message to an AMQP endpoint.
 */
fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        println!("Usage: ./example_receive localhost 5672 myqueue");
        std::process::exit(1);
    }
    let host = &args[1];
    let port = args[2].parse::<u16>().expect("Error parsing port");
    let address = &args[3];

    let opts = ConnectionOptions::new().sasl_mechanism(SaslMechanism::Anonymous);

    let container = Container::new()
        .expect("unable to create container")
        .start();

    // connect creates the TCP connection and sends OPEN frame.
    block_on(async {
        println!("Going to connect");
        let connection = container
            .connect(host, port, opts)
            .await
            .expect("connection not created");
        println!("Connection created!");

        // new_session creates the AMQP session.
        let session = connection
            .new_session(None)
            .await
            .expect("session not created");
        println!("Session created!");

        let receiver = session
            .new_receiver(address)
            .await
            .expect("receiver not created");

        let delivery = receiver.receive().await.expect("unable to receive message");

        println!("Received: {:?}", delivery.message().body);
    });
}
