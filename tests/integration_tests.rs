/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use dove::container::*;
use dove::message::MessageBody;

use futures::executor::block_on;

#[test]
fn client() {
    // Container represents an AMQP 1.0 container.
    let container = Container::new()
        .expect("unable to create container")
        .start();

    // connect creates the TCP connection and sends OPEN frame.
    block_on(async {
        println!("Going to connect");

        let opts = ConnectionOptions::new()
            .sasl_mechanism(SaslMechanism::Plain)
            .username("test")
            .password("test");
        let port: u16 = 5672;
        let connection = container
            .connect("127.0.0.1", port, opts)
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
            .new_receiver("myqueue")
            .await
            .expect("receiver not created");

        println!("Receiver created!");

        // new_sender creates the AMQP sender link.
        let sender = session
            .new_sender("myqueue")
            .await
            .expect("sender not created");

        println!("Sender created!");

        //  Send message and get disposition.
        let to_send: usize = 200;
        let mut messages = Vec::new();
        for i in 0..to_send {
            let message =
                Message::amqp_value(Value::String(format!("Hello, World: {}", i).to_string()));
            messages.push(sender.send(message));
        }
        println!("Messages send started");

        let mut deliveries = Vec::new();
        for _ in messages.iter() {
            deliveries.push(receiver.receive());
        }

        println!("Receive started");

        for message in messages.drain(..) {
            message.await.expect("error awaiting message");
        }

        println!("Messages sent");

        // Verify results
        for delivery in deliveries.drain(..) {
            println!("Joining delivery");
            let delivery = delivery.await.expect("error awaiting delivery");
            println!("Joined delivery");
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

        println!("Messages verified");
    });
}

//#[test]
/*
fn server() {
    let mut listener = listen(
        "localhost",
        5672,
        ListenOptions {
            container_id: "ce8c4a3e-96b3-11e9-9bfd-c85b7644b4a4",
        },
    )
    .expect("Error creating listener");

    let mut driver = ConnectionDriver::new();

    let connection = listener.accept().unwrap();

    driver.register(connection);

    let mut event_buffer = EventBuffer::new();
    loop {
        match driver.poll(&mut event_buffer) {
            Ok(_) => {
                for event in event_buffer.drain(..) {
                    match event {
                        Event::ConnectionInit(_) => {}
                        Event::RemoteOpen(cid, _) => {
                            println!("Remote opened!");
                            let conn = driver.connection(cid).unwrap();
                            conn.open();
                        }
                        Event::RemoteBegin(cid, chan, _) => {
                            println!("Remote begin");
                            let conn = driver.connection(cid).unwrap();

                            let session = conn.get_session(chan).unwrap();
                            session.open();
                        }
                        Event::RemoteClose(cid, _) => {
                            println!("Received close from peer, closing connection!");
                            let conn = driver.connection(cid).unwrap();
                            conn.close(None);
                        }
                        e => {
                            println!("Unhandled event: {:#?}", e);
                        }
                    }
                }
            }
            Err(e) => {
                println!("Got error: {:?}", e);
                assert!(false);
            }
        }
    }
}
*/
