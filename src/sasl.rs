/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The sasl module implements the SASL support in dove.

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
    _supported_mechanisms: Vec<SaslMechanism>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SaslState {
    InProgress,
    Success,
    Failed,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SaslMechanism {
    Anonymous,
    Plain,
    CramMd5,
    DigestMd5,
    ScramSha1,
    ScramSha256,
    Other(String),
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
        } else if "digest-md5".eq_ignore_ascii_case(s) {
            Ok(SaslMechanism::DigestMd5)
        } else if "scram-sha-1".eq_ignore_ascii_case(s) {
            Ok(SaslMechanism::ScramSha1)
        } else if "scram-sha-256".eq_ignore_ascii_case(s) {
            Ok(SaslMechanism::ScramSha256)
        } else {
            Ok(SaslMechanism::Other(s.to_string()))
        }
    }
}

impl AsRef<str> for SaslMechanism {
    fn as_ref(&self) -> &str {
        match self {
            SaslMechanism::Anonymous => "ANONYMOUS",
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::CramMd5 => "CRAM-MD5",
            SaslMechanism::DigestMd5 => "DIGEST-MD5",
            SaslMechanism::ScramSha1 => "SCRAM-SHA-1",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::Other(other) => other.as_str(),
        }
    }
}

impl ToString for SaslMechanism {
    fn to_string(&self) -> String {
        self.as_ref().to_string()
    }
}

impl Sasl {
    pub fn is_done(&self) -> bool {
        self.state == SaslState::Success || self.state == SaslState::Failed
    }

    pub fn perform_handshake<N: Network>(
        &mut self,
        hostname: Option<&str>,
        transport: &mut Transport<N>,
    ) -> Result<()> {
        match &self.role {
            SaslRole::Client(sasl_client) => {
                let frame = transport.read_frame()?;
                match frame {
                    Frame::SASL(SaslFrame::SaslMechanisms(mechs)) => {
                        trace!(
                            "Got mechs {:?}, we want: {:?}!",
                            mechs,
                            sasl_client.mechanism
                        );
                        let mut found = false;
                        for supported_mech in mechs.mechanisms.iter() {
                            if sasl_client.mechanism == *supported_mech {
                                found = true;
                            }
                        }
                        if !found {
                            self.state = SaslState::Failed;
                        } else if SaslMechanism::Plain == sasl_client.mechanism {
                            let mut data = Vec::new();
                            data.extend_from_slice(
                                sasl_client
                                    .username
                                    .as_deref()
                                    .unwrap_or_default()
                                    .as_bytes(),
                            );
                            data.push(0);
                            data.extend_from_slice(
                                sasl_client
                                    .username
                                    .as_deref()
                                    .unwrap_or_default()
                                    .as_bytes(),
                            );
                            data.push(0);
                            data.extend_from_slice(
                                sasl_client
                                    .password
                                    .as_deref()
                                    .unwrap_or_default()
                                    .as_bytes(),
                            );
                            transport.write_frame(&Frame::SASL(SaslFrame::SaslInit(SaslInit {
                                mechanism: sasl_client.mechanism.clone(),
                                initial_response: Some(data),
                                hostname: hostname.map(|s| s.to_string()),
                            })))?;
                        } else if SaslMechanism::Anonymous == sasl_client.mechanism {
                            transport.write_frame(&Frame::SASL(SaslFrame::SaslInit(SaslInit {
                                mechanism: sasl_client.mechanism.clone(),
                                // For anonymous login, this *must* be set to `Some(..)` value!
                                // `Vec::new` is const, so it shouldn't matter in the end (perf)
                                initial_response: Some(Vec::new()),
                                hostname: hostname.map(|s| s.to_string()),
                            })))?;
                        } else {
                            self.state = SaslState::Failed;
                            return Err(AmqpError::SaslMechanismNotSupported(
                                sasl_client.mechanism.clone(),
                            ));
                        }
                    }
                    Frame::SASL(SaslFrame::SaslOutcome(outcome)) => {
                        trace!("Sasl outcome {:?}", outcome);
                        if outcome.code == 0 {
                            self.state = SaslState::Success;
                        } else {
                            self.state = SaslState::Failed;
                        }
                    }
                    _ => warn!("Got unexpected frame {:?}", frame),
                }
            }
            SaslRole::Server(_) => {}
        }
        Ok(())
    }
}
