/*
 * Copyright 2019-2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use dove::container::*;
use dove::message::MessageBody;

use futures::executor::block_on;
use std::sync::Once;
use std::thread;

static INIT: Once = Once::new();

fn setup() {
    INIT.call_once(|| {
        env_logger::init();
    });
}

#[test]
fn single_client() {
    setup();
    // Container represents an AMQP 1.0 container.
    let container = Container::new()
        .expect("unable to create container")
        .start();

    // connect creates the TCP connection and sends OPEN frame.
    block_on(async {
        log::info!("{}: connecting", container.container_id());

        let opts = ConnectionOptions::new().sasl_mechanism(SaslMechanism::Anonymous);
        let port: u16 = 5672;
        let connection = container
            .connect("127.0.0.1", port, opts)
            .await
            .expect("connection not created");

        // new_session creates the AMQP session.
        let session = connection
            .new_session(None)
            .await
            .expect("session not created");

        log::info!("{}: creating receiver", container.container_id());
        let receiver = session
            .new_receiver("myqueue")
            .await
            .expect("receiver not created");

        // new_sender creates the AMQP sender link.
        log::info!("{}: creating sender", container.container_id());
        let sender = session
            .new_sender("myqueue")
            .await
            .expect("sender not created");

        //  Send message and get disposition.
        let to_send: usize = 2000;
        let mut messages = Vec::new();
        log::info!("{}: sending messages", container.container_id());
        for i in 0..to_send {
            let message =
                Message::amqp_value(Value::String(format!("Hello, World: {}", i).to_string()));
            messages.push(sender.send(message));
        }

        log::info!("{}: receiving messages", container.container_id());
        let mut deliveries = Vec::new();
        for _ in messages.iter() {
            deliveries.push(receiver.receive());
        }

        // Making sure messages are sent
        log::info!(
            "{}: waiting for message confirmation",
            container.container_id()
        );
        for message in messages.drain(..) {
            message.await.expect("error awaiting message");
        }

        // Verify results
        for delivery in deliveries.drain(..) {
            let mut delivery = delivery.await.expect("error awaiting delivery");
            if let MessageBody::AmqpValue(Value::String(ref s)) = delivery.message().body {
                assert!(s.starts_with("Hello, World"));
            } else {
                assert!(false);
            }
            // Manual disposition. If not sent, disposition settled + Accepted will be sent on delivery teardown
            delivery
                .disposition(true, DeliveryState::Accepted)
                .await
                .expect("disposition not sent");
        }

        log::info!("{}: messages verified", container.container_id());
    });
}

#[test]
fn multiple_clients() {
    setup();
    let to_send: usize = 2000;
    let t1 = thread::spawn(move || {
        block_on(async {
            let container = Container::with_id("sender")
                .expect("unable to create container")
                .start();

            let opts = ConnectionOptions::new().sasl_mechanism(SaslMechanism::Anonymous);
            let port: u16 = 5672;
            log::info!("{}: connecting", container.container_id());
            let connection = container
                .connect("127.0.0.1", port, opts)
                .await
                .expect("connection not created");

            // new_session creates the AMQP session.
            log::info!("{}: creating session", container.container_id());
            let session = connection
                .new_session(None)
                .await
                .expect("session not created");

            // new_sender creates the AMQP sender link.
            log::info!("{}: creating sender", container.container_id());
            let sender = session
                .new_sender("queue2")
                .await
                .expect("sender not created");

            log::info!("{}: sending messages", container.container_id());

            //  Send message and get disposition.
            let mut messages = Vec::new();
            for i in 0..to_send {
                let message =
                    Message::amqp_value(Value::String(format!("Hello, World: {}", i).to_string()));
                messages.push(sender.send(message));
            }

            log::info!(
                "{}: waiting for message confirmation",
                container.container_id()
            );

            // Making sure messages are sent
            for message in messages.drain(..) {
                message.await.expect("error awaiting message");
            }
        });
    });

    let t2 = thread::spawn(move || {
        block_on(async {
            let container = Container::with_id("receiver")
                .expect("unable to create container")
                .start();

            let opts = ConnectionOptions::new().sasl_mechanism(SaslMechanism::Anonymous);
            let port: u16 = 5672;

            log::info!("{}: connecting", container.container_id());
            let connection = container
                .connect("127.0.0.1", port, opts)
                .await
                .expect("connection not created");

            // new_session creates the AMQP session.
            log::info!("{}: creating session", container.container_id());
            let session = connection
                .new_session(None)
                .await
                .expect("session not created");

            log::info!("{}: creating receiver", container.container_id());
            let receiver = session
                .new_receiver("queue2")
                .await
                .expect("receiver not created");

            log::info!("{}: receiving messages", container.container_id());
            let mut deliveries = Vec::new();
            for _ in 0..to_send {
                deliveries.push(receiver.receive());
            }

            // Verify results
            log::info!("{}: verifying message contents", container.container_id());
            for delivery in deliveries.drain(..) {
                let mut delivery = delivery.await.expect("error awaiting delivery");
                if let MessageBody::AmqpValue(Value::String(ref s)) = delivery.message().body {
                    assert!(s.starts_with("Hello, World"));
                } else {
                    assert!(false);
                }
                delivery
                    .disposition(true, DeliveryState::Accepted)
                    .await
                    .expect("disposition not sent");
            }

            log::info!("{}: messages verified", container.container_id());
        });
    });

    t1.join().expect("sender error");
    t2.join().expect("receiver error");
}
