/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use dove::container::*;
use dove::url;
use futures::executor::block_on;
use std::env;
use tokio::time::Duration;

/**
 * Example client that receives a single message to an AMQP endpoint.
 */
fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 1 {
        println!("Usage: receive amqp://localhost:5672/myqueue");
        std::process::exit(1);
    }

    env_logger::init();

    let url = &args[1];
    let url = url::Url::parse(url).expect("error parsing url");
    let opts = ConnectionOptions {
        username: url.username.map(|s| s.to_string()),
        password: url.password.map(|s| s.to_string()),
        sasl_mechanism: url.username.map_or(Some(SaslMechanism::Anonymous), |_| {
            Some(SaslMechanism::Plain)
        }),
        idle_timeout: Some(Duration::from_secs(5)),
        tls_config: None
    };

    let container = Container::new()
        .expect("unable to create container")
        .start();

    // connect creates the TCP connection and sends OPEN frame.
    block_on(async {
        println!("Going to connect");
        let connection = container
            .connect(url.hostname, url.port, opts)
            .await
            .expect("connection not created");

        println!("Connected");

        // new_session creates the AMQP session.
        let session = connection
            .new_session(None)
            .await
            .expect("session not created");

        println!("Session created");

        let receiver = session
            .new_receiver(url.address)
            .await
            .expect("receiver not created");

        println!("Receiver created");

        let delivery = receiver.receive().await.expect("unable to receive message");

        println!("Received: {:?}", delivery.message().body);
    });
}
