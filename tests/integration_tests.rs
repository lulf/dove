/*
 * Copyright 2019-2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use dove::container::*;
use dove::error::AmqpError;
use dove::message::MessageBody;

use futures::future::join_all;
use std::process::Command;
use std::sync::Once;
use std::time::{Duration, Instant};
use testcontainers::{clients, images, Docker};
use tokio::time::{sleep, timeout};

static INIT: Once = Once::new();

fn setup() {
    INIT.call_once(|| {
        env_logger::init();
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn test_artemis() {
    setup();
    let docker = clients::Cli::default();
    let node = docker.run(
        images::generic::GenericImage::new("docker.io/vromero/activemq-artemis:2-latest")
            .with_env_var("ARTEMIS_USERNAME", "test")
            .with_env_var("ARTEMIS_PASSWORD", "test"),
    );
    let id = node.id();
    log::info!("ActiveMQ Artemis container started with id {}", id);
    sleep(Duration::from_millis(30000)).await;
    let port: u16 = node.get_host_port(5672).unwrap();

    timeout(Duration::from_secs(120), async move {
        let opts = ConnectionOptions::new()
            .sasl_mechanism(SaslMechanism::Plain)
            .username("test")
            .password("test")
            .idle_timeout(Duration::from_secs(5));
        single_client(port, opts.clone()).await;
        multiple_clients(port, opts).await;
    })
    .await
    .unwrap_or_else(|_| {
        log::info!("ActiveMQ Artemis test timed out");
        print_docker_log(id);
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn test_qpid_dispatch() {
    setup();
    let docker = clients::Cli::default();
    let node = docker.run(images::generic::GenericImage::new(
        "quay.io/interconnectedcloud/qdrouterd:1.12.0",
    ));
    let id = node.id();
    log::info!("Router container started with id {}", id);
    sleep(Duration::from_millis(10000)).await;
    let port: u16 = node.get_host_port(5672).unwrap();

    timeout(Duration::from_secs(120), async move {
        let opts = ConnectionOptions::new()
            .sasl_mechanism(SaslMechanism::Anonymous)
            .idle_timeout(Duration::from_secs(5));
        multiple_clients(port, opts).await;
    })
    .await
    .unwrap_or_else(|_| {
        log::info!("Router test timed out");
        print_docker_log(id);
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn test_qpid_broker_j() {
    setup();

    let mut config_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    config_dir.push("tests");
    config_dir.push("qpid-broker-j");
    log::info!("Loaded config from {:?}", config_dir);
    let docker = clients::Cli::default();
    let config_dir_path = config_dir.as_path().to_str().unwrap();

    // Required to allow reading directory in container
    if std::env::consts::OS == "linux" {
        let chcon_output = std::process::Command::new("chcon")
            .arg("-t")
            .arg("svirt_sandbox_file_t")
            .arg(config_dir_path)
            .output()
            .expect("failed to run command");
        log::info!("CHCON: {:?}", chcon_output);
    }

    let node = docker.run(
        images::generic::GenericImage::new("docker.io/chrisob/qpid-broker-j-docker:8.0.0")
            .with_volume(config_dir_path, "/usr/local/etc"),
    );
    let id = node.id();
    log::info!("Qpid Broker J container started with id {}", id);

    sleep(Duration::from_millis(20000)).await;
    let http_port: u16 = node.get_host_port(8080).unwrap();
    let port: u16 = node.get_host_port(5672).unwrap();

    timeout(Duration::from_secs(120), async move {
        // Create queues used by tests
        let client = reqwest::Client::new();
        create_queue(&client, http_port, "myqueue").await;
        create_queue(&client, http_port, "queue2").await;

        let opts = ConnectionOptions::new()
            .sasl_mechanism(SaslMechanism::Anonymous)
            .idle_timeout(Duration::from_secs(5));
        single_client(port, opts.clone()).await;
        multiple_clients(port, opts).await;
    })
    .await
    .unwrap_or_else(|_| {
        log::info!("Qpid Broker J test timed out");
        print_docker_log(id);
    });
}

fn print_docker_log(id: &str) {
    let command = Command::new("docker")
        .arg("logs")
        .arg(id)
        .output()
        .expect("failed to execute docker logs");
    log::info!("{}", String::from_utf8(command.stdout).unwrap());
}

async fn single_client(port: u16, opts: ConnectionOptions) {
    // Container represents an AMQP 1.0 container.
    let container = Container::new()
        .expect("unable to create container")
        .start();

    // connect creates the TCP connection and sends OPEN frame.
    log::info!("{}: connecting on port {}", container.container_id(), port);

    let connection = container
        .connect(
            "127.0.0.1", 
            port,
            opts,
        )
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
        let mut message =
            Message::amqp_value(Value::String(format!("Hello, World: {}", i).to_string()));

        let start = Instant::now();
        messages.push(loop {
            // a future does nothing unless polled / awaited ...
            match sender.send(message).await {
                Err(AmqpError::NotEnoughCreditsToSend(m)) => {
                    if start.elapsed() > Duration::from_secs(5) {
                        panic!("Did not receive enough credits within timeout to send message",);
                    } else {
                        sleep(Duration::from_millis(100)).await;
                        message = *m;
                    }
                }
                Err(e) => panic!("Failed to send message: {:?}", e),
                Ok(result) => break result,
            }
        });
    }

    log::info!("{}: receiving messages", container.container_id());
    let mut deliveries = Vec::new();
    for _ in messages.iter() {
        deliveries.push(receiver.receive());
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
}

async fn multiple_clients(port: u16, opts: ConnectionOptions) {
    let to_send: usize = 2000;
    let sender_opts = opts.clone();

    let t1 = tokio::spawn(async move {
        let container = Container::with_id("sender")
            .expect("unable to create container")
            .start();
        log::info!("{}: connecting on port {}", container.container_id(), port);
        let connection = container
            .connect(
                "127.0.0.1", 
                port,
                sender_opts,
            )
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

    let t2 = tokio::spawn(async move {
        let container = Container::with_id("receiver")
            .expect("unable to create container")
            .start();

        log::info!("{}: connecting on port {}", container.container_id(), port);
        let connection = container
            .connect(
                "127.0.0.1", 
                port,
                opts,
            )
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

    join_all(vec![t1, t2]).await;
}

async fn create_queue(client: &reqwest::Client, port: u16, queue: &str) {
    let url = format!(
        "http://localhost:{}/api/latest/queue/default/default/{}",
        port, queue
    );
    let response = client
        .put(&url)
        .body("{\"durable\":true}")
        .send()
        .await
        .expect("error creating queue");
    log::trace!("Create queue response: {:?}", response);
    assert!(response.status().is_success());
}
