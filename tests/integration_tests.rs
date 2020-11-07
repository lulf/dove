/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

extern crate dove;

use dove::client::*;
use dove::conn::*;
use dove::driver::*;
use dove::error::*;
use dove::framing::*;
use dove::sasl::*;
use futures::executor::block_on;
use mio::{Events, Interest, Poll, Registry, Token};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{thread, time};

/*
#[test]
fn client() {
    //let opts = ConnectionOptions::new().sasl_mechanism(SaslMechanism::Anonymous);
    let opts = ConnectionOptions::new()
        .sasl_mechanism(SaslMechanism::Plain)
        .username("test")
        .password("test");

    let mut poll = Poll::new().expect("error creating poller");
    let mut events = Events::with_capacity(1024);

    let connection = connect("127.0.0.1", 5672, opts).expect("error opening connection");
    let mut driver = ConnectionDriver::new(1, connection);
    let id = driver.token();

    poll.registry()
        .register(&mut driver, id, Interest::READABLE | Interest::WRITABLE)
        .expect("error registering connection");

    let mut event_buffer = EventBuffer::new();
    let mut sent = false;
    let mut done = false;

    while !done {
        poll.poll(&mut events, Some(Duration::from_secs(5)))
            .expect("error during poll");
        for event in &events {
            if event.token() == id {
                // Do work until we get blocked
                while !done {
                    let result = driver.do_work(&mut event_buffer);
                    match result {
                        Err(AmqpError::IoError(ref e))
                            if e.kind() == std::io::ErrorKind::WouldBlock =>
                        {
                            break;
                        }
                        Err(e) => {
                            println!("Got error: {:?}", e);
                            assert!(false);
                        }
                        Ok(_) => {
                            for event in event_buffer.drain(..) {
                                match event {
                                    Event::ConnectionInit(cid) => {
                                        println!("Opening connection!");
                                        driver.open();
                                    }
                                    Event::RemoteOpen(cid, _) => {
                                        println!("Remote opened!");
                                        let session = driver.create_session();
                                        session.open();
                                    }
                                    Event::RemoteBegin(cid, chan, _) => {
                                        println!("Remote begin");
                                        let session = driver.get_session(chan).unwrap();
                                        let receiver = session.create_receiver(Some("a"));
                                        receiver.open();
                                    }
                                    Event::RemoteAttach(cid, chan, handle, _) => {
                                        let session = driver.get_session(chan).unwrap();
                                        let link = session.get_link(handle).unwrap();
                                        if link.role == LinkRole::Receiver {
                                            link.flow(10);
                                            let sender = session.create_sender(Some("a"));
                                            sender.open();
                                        }
                                    }
                                    Event::Flow(cid, chan, handle, flow) => {
                                        println!("Received flow ({:?} credits)", flow.link_credit);
                                        let session = driver.get_session(chan).unwrap();
                                        let link = session.get_link(handle).unwrap();
                                        if link.role == LinkRole::Sender && !sent {
                                            println!("Sending message!");
                                            link.send("Hello, World");
                                            sent = true;
                                        }
                                    }
                                    Event::Delivery(cid, chan, handle, delivery) => {
                                        println!("Received message: {:?}", delivery.message.body);
                                        let session = driver.get_session(chan).unwrap();
                                        let link = session.get_link(handle).unwrap();
                                        link.settle(&delivery, true, DeliveryState::Accepted);
                                        driver.close(None);
                                    }
                                    Event::Disposition(_, _, disposition) => {
                                        if let Some(settled) = disposition.settled {
                                            if let Some(state) = disposition.state {
                                                if settled && state == DeliveryState::Accepted {
                                                    println!("Message delivered!");
                                                } else {
                                                    println!("Error delivering message!");
                                                }
                                            }
                                        }
                                    }
                                    Event::RemoteClose(cid, close) => {
                                        println!(
                                            "Received close from peer ({:?}), closing connection!",
                                            close
                                        );
                                        driver.close(None);
                                        done = true;
                                    }
                                    e => {
                                        println!("Unhandled event: {:#?}", e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
*/

#[test]
fn client_async() {
    // Client handle represents an AMQP 1.0 container.
    let client = Arc::new(Client::new().expect("unable to create client"));

    // connect creates the TCP connection and sends OPEN frame.
    block_on(async {
        let c = client.clone();
        thread::spawn(move || loop {
            c.process();
        });

        println!("Going to connect");

        let opts = ConnectionOptions::new()
            .sasl_mechanism(SaslMechanism::Plain)
            .username("test")
            .password("test");
        let port: u16 = 5672;
        let connection = client
            .connect("127.0.0.1", port, opts)
            .await
            .expect("connection not created");
        println!("Connection created!");

        // new_session creates the AMQP session.
        let session = connection.new_session().await.expect("session not created");
        println!("Session created!");

        // new_sender creates the AMQP sender link.
        let sender = session
            .new_sender("myqueue")
            .await
            .expect("sender not created");

        //  Send message and get disposition.
        let disposition = sender
            .send("Hello, World")
            .await
            .expect("disposition not received");

        let receiver = session
            .new_receiver("myqueue")
            .await
            .expect("receiver not created");

        let delivery = receiver.receive().await.expect("unable to receive message");

        let message = delivery.message().expect("unable to decode message");
        assert!(message == "Hello, World");
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
