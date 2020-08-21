/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

use std::str::FromStr;

use crate::error::*;
use crate::framing::*;
use crate::transport::*;

#[derive(Debug)]
pub struct Sasl {
    pub role: SaslRole,
    pub state: SaslState,
}

#[derive(Debug)]
pub enum SaslRole {
    Server(SaslServer),
    Client(SaslClient),
}

#[derive(Debug)]
pub struct SaslClient {
    pub mechanism: SaslMechanism,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug)]
pub struct SaslServer {
    supported_mechanisms: Vec<SaslMechanism>,
}

#[derive(Debug, PartialEq)]
pub enum SaslState {
    InProgress,
    Success,
    Failed,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SaslMechanism {
    Anonymous,
    Plain,
    CramMd5,
    ScramSha1,
    ScramSha256,
}

impl FromStr for SaslMechanism {
    type Err = AmqpError;
    fn from_str(s: &str) -> Result<SaslMechanism> {
        if "anonymous".eq_ignore_ascii_case(s) {
            Ok(SaslMechanism::Anonymous)
        } else if "plain".eq_ignore_ascii_case(s) {
            Ok(SaslMechanism::Plain)
        } else if "cram-md5".eq_ignore_ascii_case(s) {
            Ok(SaslMechanism::CramMd5)
        } else if "scram-sha-1".eq_ignore_ascii_case(s) {
            Ok(SaslMechanism::ScramSha1)
        } else if "scram-sha-256".eq_ignore_ascii_case(s) {
            Ok(SaslMechanism::ScramSha256)
        } else {
            Err(AmqpError::decode_error(Some(
                format!("Unknown SASL mechanism {}", s).as_str(),
            )))
        }
    }
}

impl ToString for SaslMechanism {
    fn to_string(&self) -> String {
        match self {
            SaslMechanism::Anonymous => "ANONYMOUS".to_string(),
            SaslMechanism::Plain => "PLAIN".to_string(),
            _ => panic!("Unsupported mechanism!"),
        }
    }
}

impl Sasl {
    pub fn is_done(self: &Self) -> bool {
        self.state == SaslState::Success || self.state == SaslState::Failed
    }

    pub fn perform_handshake(self: &mut Self, transport: &mut Transport) -> Result<()> {
        match &self.role {
            SaslRole::Client(sasl_client) => {
                let frame = transport.read_frame()?;
                match frame {
                    Frame::SASL(SaslFrame::SaslMechanisms(mechs)) => {
                        println!(
                            "Got mechs {:?}, we want: {:?}!",
                            mechs, sasl_client.mechanism
                        );
                        let mut found = false;
                        for supported_mech in mechs.iter() {
                            if sasl_client.mechanism == *supported_mech {
                                println!("Found supported mechanism, proceed!");
                                found = true;
                            }
                        }
                        if !found {
                            println!("Unable to find supported mechanism");
                            self.state = SaslState::Failed;
                        } else {
                            let mut initial_response = None;
                            if sasl_client.mechanism == SaslMechanism::Plain {
                                let mut data = Vec::new();
                                data.extend_from_slice(
                                    sasl_client.username.clone().unwrap().as_bytes(),
                                );
                                data.push(0);
                                data.extend_from_slice(
                                    sasl_client.username.clone().unwrap().as_bytes(),
                                );
                                data.push(0);
                                data.extend_from_slice(
                                    sasl_client.password.clone().unwrap().as_bytes(),
                                );
                                initial_response = Some(data);
                            }
                            let init = Frame::SASL(SaslFrame::SaslInit(SaslInit {
                                mechanism: sasl_client.mechanism,
                                initial_response: initial_response,
                                hostname: None,
                            }));
                            transport.write_frame(&init)?;
                        }
                    }
                    Frame::SASL(SaslFrame::SaslOutcome(outcome)) => {
                        println!("Got outcome: {:?}", outcome);
                        if outcome.code == 0 {
                            self.state = SaslState::Success;
                        } else {
                            self.state = SaslState::Failed;
                        }
                    }
                    _ => println!("Got frame {:?}", frame),
                }
            }
            SaslRole::Server(_) => {}
        }
        Ok(())
    }
}
