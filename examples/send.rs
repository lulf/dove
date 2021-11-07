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
 * Example client that sends a single message to an AMQP endpoint.
 */
fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Usage: send amqp://localhost:5672/myqueue 'Hello, world'");
        std::process::exit(1);
    }

    env_logger::init();

    let url = &args[1];
    let data = &args[2];

    let url = url::Url::parse(url).expect("error parsing url");
    let opts = ConnectionOptions {
        username: url.username.map(|s| s.to_string()),
        password: url.password.map(|s| s.to_string()),
        sasl_mechanism: url.username.map_or(Some(SaslMechanism::Anonymous), |_| {
            Some(SaslMechanism::Plain)
        }),
        idle_timeout: Some(Duration::from_secs(5)),
    };

    let container = Container::new()
        .expect("unable to create container")
        .start();

    // connect creates the TCP connection and sends OPEN frame.
    block_on(async {
        let connection = container
            .connect(url.hostname, url.port, opts)
            .await
            .expect("connection not created");

        // new_session creates the AMQP session.
        let session = connection
            .new_session(None)
            .await
            .expect("session not created");

        // new_sender creates the AMQP sender link.
        let sender = session
            .new_sender(&url.address)
            .await
            .expect("sender not created");

        //  Send message and get delivery.
        let message = Message::amqp_value(Value::String(data.to_string()));
        let _ = sender.send(message).await.expect("delivery not received");

        println!("Message sent!");
    });
}
