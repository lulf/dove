/*
 * Copyright 2020, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The message module implements the AMQP 1.0 message format encoding and decoding.

use byteorder::WriteBytesExt;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::io::Write;
use std::vec::Vec;

use crate::decoding::*;
use crate::error::*;
use crate::frame_codec::*;
use crate::symbol::*;
use crate::types::*;

#[derive(Debug, Clone)]
pub struct Message {
    pub header: Option<MessageHeader>,
    pub delivery_annotations: Option<BTreeMap<Value, Value>>,
    pub message_annotations: Option<BTreeMap<Value, Value>>,
    pub properties: Option<MessageProperties>,
    pub application_properties: Option<BTreeMap<Value, Value>>,
    pub body: MessageBody,
    pub footer: Option<BTreeMap<Value, Value>>,
}

#[derive(Debug, Clone)]
pub struct MessageHeader {
    pub durable: Option<bool>,
    pub priority: Option<u8>,
    pub ttl: Option<u32>,
    pub first_acquirer: Option<bool>,
    pub delivery_count: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct MessageProperties {
    pub message_id: Option<Value>,
    pub user_id: Option<Vec<u8>>,
    pub to: Option<String>,
    pub subject: Option<String>,
    pub reply_to: Option<String>,
    pub correlation_id: Option<Value>,
    pub content_type: Option<Symbol>,
    pub content_encoding: Option<Symbol>,
    pub absolute_expiry_time: Option<u64>,
    pub creation_time: Option<u64>,
    pub group_id: Option<String>,
    pub group_sequence: Option<u32>,
    pub reply_to_group_id: Option<String>,
}

#[derive(Debug, Clone)]
pub enum MessageBody {
    AmqpSequence(Vec<Value>),
    Data(Vec<u8>),
    AmqpValue(Value),
}

impl Message {
    pub fn amqp_value(value: Value) -> Message {
        Message {
            header: Some(MessageHeader {
                durable: Some(false),
                priority: Some(4),
                ttl: None,
                first_acquirer: Some(false),
                delivery_count: Some(0),
            }),
            delivery_annotations: None,
            message_annotations: None,
            properties: None,
            application_properties: None,
            body: MessageBody::AmqpValue(value),
            footer: None,
        }
    }

    pub fn decode(reader: &mut Vec<u8>) -> Result<Message> {
        let len = reader.len() as u64;
        let mut cursor = Cursor::new(reader);
        let mut message = Message {
            header: None,
            delivery_annotations: None,
            message_annotations: None,
            properties: None,
            application_properties: None,
            body: MessageBody::AmqpValue(Value::Null),
            footer: None,
        };
        while cursor.position() < len {
            if let Value::Described(descriptor, mut value) = decode_value(&mut cursor)? {
                match *descriptor {
                    DESC_MESSAGE_HEADER => {
                        let decoder = FrameDecoder::new(&descriptor, &mut value)?;
                        message.header = Some(MessageHeader::decode(decoder)?);
                    }
                    DESC_MESSAGE_DELIVERY_ANNOTATIONS => {
                        if let Value::Map(m) = *value {
                            message.delivery_annotations = Some(m);
                        }
                    }

                    DESC_MESSAGE_ANNOTATIONS => {
                        if let Value::Map(m) = *value {
                            message.message_annotations = Some(m);
                        }
                    }
                    DESC_MESSAGE_PROPERTIES => {
                        let decoder = FrameDecoder::new(&descriptor, &mut value)?;
                        message.properties = Some(MessageProperties::decode(decoder)?);
                    }
                    DESC_MESSAGE_APPLICATION_PROPERTIES => {
                        if let Value::Map(m) = *value {
                            message.application_properties = Some(m);
                        }
                    }
                    DESC_MESSAGE_AMQP_DATA => {
                        if let Value::Binary(d) = *value {
                            message.body = MessageBody::Data(d);
                        }
                    }
                    DESC_MESSAGE_AMQP_SEQUENCE => {
                        if let Value::List(l) = *value {
                            message.body = MessageBody::AmqpSequence(l);
                        }
                    }
                    DESC_MESSAGE_AMQP_VALUE => {
                        message.body = MessageBody::AmqpValue(*value);
                    }
                    DESC_MESSAGE_FOOTER => {
                        if let Value::Map(m) = *value {
                            message.footer = Some(m);
                        }
                    }
                    _ => {
                        return Err(AmqpError::framing_error());
                    }
                }
            } else {
                break;
            }
        }
        Ok(message)
    }

    pub fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        if let Some(ref header) = self.header {
            header.encode(writer)?;
        }

        if let Some(ref delivery_annotations) = self.delivery_annotations {
            delivery_annotations.encode(writer)?;
        }
        if let Some(ref message_annotations) = self.message_annotations {
            message_annotations.encode(writer)?;
        }
        if let Some(ref properties) = self.properties {
            properties.encode(writer)?;
        }
        if let Some(ref application_properties) = self.application_properties {
            application_properties.encode(writer)?;
        }

        self.body.encode(writer)?;

        if let Some(ref footer) = self.footer {
            footer.encode(writer)?;
        }
        Ok(())
    }
}

impl MessageHeader {
    pub fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        let mut encoder = FrameEncoder::new(DESC_MESSAGE_HEADER);
        encoder.encode_arg(&self.durable.unwrap_or(false))?;
        encoder.encode_arg(&self.priority.unwrap_or(4))?;
        encoder.encode_arg(&self.ttl)?;
        encoder.encode_arg(&self.first_acquirer.unwrap_or(false))?;
        encoder.encode_arg(&self.delivery_count.unwrap_or(0))?;
        encoder.encode(writer)?;
        Ok(())
    }

    pub fn decode(mut decoder: FrameDecoder) -> Result<MessageHeader> {
        let mut header = MessageHeader {
            durable: None,
            priority: None,
            ttl: None,
            first_acquirer: None,
            delivery_count: None,
        };
        decoder.decode_optional(&mut header.durable)?;
        decoder.decode_optional(&mut header.priority)?;
        decoder.decode_optional(&mut header.ttl)?;
        decoder.decode_optional(&mut header.first_acquirer)?;
        decoder.decode_optional(&mut header.delivery_count)?;
        Ok(header)
    }
}

impl MessageProperties {
    pub fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        let mut encoder = FrameEncoder::new(DESC_MESSAGE_PROPERTIES);
        encoder.encode_arg(&self.message_id)?;
        encoder.encode_arg(&self.user_id)?;
        encoder.encode_arg(&self.to)?;
        encoder.encode_arg(&self.subject)?;
        encoder.encode_arg(&self.reply_to)?;
        encoder.encode_arg(&self.correlation_id)?;
        encoder.encode_arg(&self.content_type)?;
        encoder.encode_arg(&self.content_encoding)?;
        encoder.encode_arg(&self.absolute_expiry_time)?;
        encoder.encode_arg(&self.creation_time)?;
        encoder.encode_arg(&self.group_id)?;
        encoder.encode_arg(&self.group_sequence)?;
        encoder.encode_arg(&self.reply_to_group_id)?;
        encoder.encode(writer)?;
        Ok(())
    }

    pub fn decode(mut decoder: FrameDecoder) -> Result<MessageProperties> {
        let mut properties = MessageProperties {
            message_id: None,
            user_id: None,
            to: None,
            subject: None,
            reply_to: None,
            correlation_id: None,
            content_type: None,
            content_encoding: None,
            absolute_expiry_time: None,
            creation_time: None,
            group_id: None,
            group_sequence: None,
            reply_to_group_id: None,
        };
        decoder.decode_optional(&mut properties.message_id)?;
        decoder.decode_optional(&mut properties.user_id)?;
        decoder.decode_optional(&mut properties.to)?;
        decoder.decode_optional(&mut properties.subject)?;
        decoder.decode_optional(&mut properties.reply_to)?;
        decoder.decode_optional(&mut properties.correlation_id)?;
        decoder.decode_optional(&mut properties.content_type)?;
        decoder.decode_optional(&mut properties.content_encoding)?;
        decoder.decode_optional(&mut properties.absolute_expiry_time)?;
        decoder.decode_optional(&mut properties.creation_time)?;
        decoder.decode_optional(&mut properties.group_id)?;
        decoder.decode_optional(&mut properties.group_sequence)?;
        decoder.decode_optional(&mut properties.reply_to_group_id)?;
        Ok(properties)
    }
}

impl MessageBody {
    pub fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        match self {
            MessageBody::AmqpValue(value) => {
                writer.write_u8(0)?;
                DESC_MESSAGE_AMQP_VALUE.encode(writer)?;
                value.encode(writer)?;
            }
            MessageBody::AmqpSequence(values) => {
                writer.write_u8(0)?;
                DESC_MESSAGE_AMQP_SEQUENCE.encode(writer)?;
                for value in values.iter() {
                    value.encode(writer)?;
                }
            }
            MessageBody::Data(data) => {
                writer.write_u8(0)?;
                DESC_MESSAGE_AMQP_DATA.encode(writer)?;
                ValueRef::Binary(data).encode(writer)?;
            }
        }
        Ok(())
    }
}
