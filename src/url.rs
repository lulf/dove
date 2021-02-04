/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! Utility module for working with AMQP 1.0 URLs similar to that supported by Apache Qpid.

use crate::error::*;

#[derive(PartialEq, Debug)]
pub struct Url<'a> {
    pub scheme: UrlScheme,
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
    pub hostname: &'a str,
    pub port: u16,
    pub address: &'a str,
}

#[derive(PartialEq, Debug)]
pub enum UrlScheme {
    AMQP,
    AMQPS,
}

impl UrlScheme {
    fn parse(input: &str) -> (&str, Result<UrlScheme>) {
        if let Some(s) = input.strip_prefix("amqp://") {
            (s, Ok(UrlScheme::AMQP))
        } else if let Some(s) = input.strip_prefix("amqps://") {
            (s, Ok(UrlScheme::AMQPS))
        } else {
            (input, Err(AmqpError::generic("unable to detect scheme")))
        }
    }
}

impl Url<'_> {
    pub fn parse(input: &str) -> Result<Url> {
        let (input, scheme) = UrlScheme::parse(input);
        let scheme = scheme?;

        let (input, username, password) = if let Some(creds_end) = input.find('@') {
            let s = &input[0..creds_end];
            if let Some(user_end) = s.find(':') {
                let u = &input[0..user_end];
                let p = &input[user_end + 1..creds_end];
                (&input[creds_end + 1..], Some(u), Some(p))
            } else {
                (input, None, None)
            }
        } else {
            (input, None, None)
        };

        let (input, hostname, port) = if let Some(pos) = input.find(':') {
            let (hostname, input) = (&input[0..pos], &input[pos + 1..]);
            let (input, port) = if let Some(end_pos) = input.find('/') {
                (&input[end_pos + 1..], (&input[..end_pos]).parse::<u16>()?)
            } else {
                ("", input.parse::<u16>()?)
            };
            (input, hostname, port)
        } else if let Some(pos) = input.find('/') {
            (&input[pos + 1..], &input[0..pos], 5672_u16)
        } else {
            ("", input, 5672_u16)
        };

        let address = if let Some(pos) = input.find('/') {
            &input[pos + 1..]
        } else {
            input
        };

        Ok(Url {
            scheme,
            username,
            password,
            hostname,
            port,
            address,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_simple() {
        let url = Url::parse(r"amqp://localhost/myqueue").expect("error parsing");
        assert_eq!(UrlScheme::AMQP, url.scheme);
        assert_eq!(5672, url.port);
        assert_eq!("localhost", url.hostname);
        assert_eq!("myqueue", url.address);
    }

    #[test]
    fn test_credentials() {
        let url = Url::parse(r"amqp://foo:bar@localhost/myqueue").expect("error parsing");
        assert_eq!(UrlScheme::AMQP, url.scheme);
        assert_eq!(5672, url.port);
        assert_eq!("localhost", url.hostname);
        assert_eq!("myqueue", url.address);
        assert_eq!("foo", url.username.unwrap());
        assert_eq!("bar", url.password.unwrap());
    }

    #[test]
    fn test_port() {
        let url = Url::parse(r"amqp://localhost:56722/myqueue").expect("error parsing");
        assert_eq!(UrlScheme::AMQP, url.scheme);
        assert_eq!(56722, url.port);
        assert_eq!("localhost", url.hostname);
        assert_eq!("myqueue", url.address);
    }

    #[test]
    fn test_nodest() {
        let url = Url::parse(r"amqp://localhost").expect("error parsing");
        assert_eq!(UrlScheme::AMQP, url.scheme);
    }

    #[test]
    fn test_full() {
        let url = Url::parse(r"amqp://foo:bar@localhost:56722/myqueue").expect("error parsing");
        assert_eq!("myqueue", url.address);
        assert_eq!(UrlScheme::AMQP, url.scheme);
    }
}
