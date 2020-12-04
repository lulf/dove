/*
 * Copyright 2019-2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use dove::container::*;
use dove::message::MessageBody;

use futures::executor::block_on;
use reqwest;
use std::sync::Once;
use std::{sync::mpsc, thread, time::Duration};
use testcontainers::{clients, images, Docker};

static INIT: Once = Once::new();

fn setup() {
    INIT.call_once(|| {
        env_logger::init();
    });
}

#[test]
fn test_artemis() {
    panic_after(Duration::from_secs(60), || {
        setup();
        let docker = clients::Cli::default();
        let node = docker.run(
            images::generic::GenericImage::new("docker.io/vromero/activemq-artemis:2-latest")
                .with_env_var("ARTEMIS_USERNAME", "test")
                .with_env_var("ARTEMIS_PASSWORD", "test"),
        );
        log::info!("ActiveMQ Artemis Started");
        std::thread::sleep(Duration::from_millis(10000));
        let port: u16 = node.get_host_port(5672).unwrap();
        let opts = ConnectionOptions::new()
            .sasl_mechanism(SaslMechanism::Plain)
            .username("test")
            .password("test");
        single_client(port, opts.clone());
        multiple_clients(port, opts);
    });
}

#[test]
fn test_qpid_dispatch() {
    panic_after(Duration::from_secs(60), || {
        setup();
        let docker = clients::Cli::default();
        let node = docker.run(images::generic::GenericImage::new(
            "quay.io/interconnectedcloud/qdrouterd:1.12.0",
        ));
        log::info!("Router Started");
        std::thread::sleep(Duration::from_millis(5000));
        let port: u16 = node.get_host_port(5672).unwrap();
        let opts = ConnectionOptions::new().sasl_mechanism(SaslMechanism::Anonymous);
        multiple_clients(port, opts);
    });
}

#[test]
fn test_qpid_broker_j() {
    panic_after(Duration::from_secs(60), || {
        setup();
        let mut config_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        config_dir.push("tests");
        config_dir.push("qpid-broker-j");
        let docker = clients::Cli::default();
        let node = docker.run(
            images::generic::GenericImage::new("docker.io/chrisob/qpid-broker-j-docker:8.0.0")
                .with_volume(config_dir.as_path().to_str().unwrap(), "/usr/local/etc"),
        );
        log::info!("Qpid Broker J Started");
        std::thread::sleep(Duration::from_millis(10000));

        // Create queues used by tests
        let client = reqwest::blocking::Client::new();
        let http_port: u16 = node.get_host_port(8080).unwrap();
        create_queue(&client, http_port, "myqueue");
        create_queue(&client, http_port, "queue2");

        let port: u16 = node.get_host_port(5672).unwrap();
        let opts = ConnectionOptions::new().sasl_mechanism(SaslMechanism::Anonymous);
        single_client(port, opts.clone());
        multiple_clients(port, opts);
    });
}

fn single_client(port: u16, opts: ConnectionOptions) {
    // Container represents an AMQP 1.0 container.
    let container = Container::new()
        .expect("unable to create container")
        .start();

    // connect creates the TCP connection and sends OPEN frame.
    block_on(async {
        log::info!("{}: connecting on port {}", container.container_id(), port);

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

fn multiple_clients(port: u16, opts: ConnectionOptions) {
    let to_send: usize = 2000;
    let sender_opts = opts.clone();
    let t1 = thread::spawn(move || {
        block_on(async {
            let container = Container::with_id("sender")
                .expect("unable to create container")
                .start();

            log::info!("{}: connecting on port {}", container.container_id(), port);
            let connection = container
                .connect("127.0.0.1", port, sender_opts)
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

            log::info!("{}: connecting on port {}", container.container_id(), port);
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

fn create_queue(client: &reqwest::blocking::Client, port: u16, queue: &str) {
    let url = format!(
        "http://localhost:{}/api/latest/queue/default/default/{}",
        port, queue
    );
    let response = client
        .put(&url)
        .body("{\"durable\":true}")
        .send()
        .expect("error creating queue");
    log::trace!("Create queue response: {:?}", response);
    assert!(response.status().is_success());
}

fn panic_after<T, F>(d: Duration, f: F) -> T
where
    T: Send + 'static,
    F: FnOnce() -> T,
    F: Send + 'static,
{
    let (done_tx, done_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let val = f();
        done_tx.send(()).expect("Unable to send completion signal");
        val
    });

    match done_rx.recv_timeout(d) {
        Ok(_) => handle.join().expect("Thread panicked"),
        Err(_) => panic!("Thread took too long"),
    }
}
