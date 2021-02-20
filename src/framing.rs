/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

//! The framing module implements the different AMQP 1.0 frame type model, encoder and decoder.

use byteorder::NetworkEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::vec::Vec;

use crate::convert::*;
use crate::decoding::*;
use crate::error::*;
use crate::frame_codec::*;
use crate::sasl::*;
use crate::symbol::*;
use crate::types::*;

#[derive(Debug)]
pub struct FrameHeader {
    pub size: u32,
    doff: u8,
    frame_type: u8,
    ext: u16,
}

#[derive(Debug)]
pub enum Frame {
    AMQP(AmqpFrame),
    SASL(SaslFrame),
}

/** SASL frame types */
#[derive(Debug)]
pub enum SaslFrame {
    SaslMechanisms(SaslMechanisms),
    SaslInit(SaslInit),
    SaslChallenge(SaslChallenge),
    SaslResponse(SaslResponse),
    SaslOutcome(SaslOutcome),
}

#[derive(Debug)]
pub struct SaslMechanisms {
    pub mechanisms: Vec<SaslMechanism>,
}

#[derive(Debug)]
pub struct SaslInit {
    pub mechanism: SaslMechanism,
    pub initial_response: Option<Vec<u8>>,
    pub hostname: Option<String>,
}

pub type SaslChallenge = Vec<u8>;
pub type SaslResponse = Vec<u8>;

#[derive(Debug)]
pub struct SaslOutcome {
    pub code: SaslCode,
    pub additional_data: Option<Vec<u8>>,
}

pub type SaslCode = u8;

impl Encoder for SaslMechanism {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let value = match self {
            SaslMechanism::Anonymous => Symbol::from_slice(b"ANONYMOUS"),
            SaslMechanism::Plain => Symbol::from_slice(b"PLAIN"),
            SaslMechanism::CramMd5 => Symbol::from_slice(b"CRAM-MD5"),
            SaslMechanism::ScramSha1 => Symbol::from_slice(b"SCRAM-SHA-1"),
            SaslMechanism::ScramSha256 => Symbol::from_slice(b"SCRAM-SHA-256"),
        };
        value.encode(writer)
    }
}

impl Encoder for SaslInit {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_SASL_INIT);
        encoder.encode_arg(&self.mechanism)?;
        encoder.encode_arg(&self.initial_response)?;
        encoder.encode_arg(&self.hostname)?;
        encoder.encode(writer)
    }
}

impl SaslOutcome {
    pub fn decode(mut decoder: FrameDecoder) -> Result<SaslOutcome> {
        let mut outcome = SaslOutcome {
            code: 4,
            additional_data: None,
        };
        decoder.decode_required(&mut outcome.code)?;
        decoder.decode_optional(&mut outcome.additional_data)?;
        Ok(outcome)
    }
}

impl SaslMechanism {
    pub fn from_slice(data: &[u8]) -> Result<SaslMechanism> {
        let input = std::str::from_utf8(data)?;
        match input {
            "ANONYMOUS" => Ok(SaslMechanism::Anonymous),
            "PLAIN" => Ok(SaslMechanism::Plain),
            "CRAM-MD5" => Ok(SaslMechanism::CramMd5),
            "SCRAM-SHA-1" => Ok(SaslMechanism::ScramSha1),
            "SCRAM-SHA-256" => Ok(SaslMechanism::ScramSha256),
            v => Err(AmqpError::decode_error(Some(
                format!("Unsupported SASL mechanism {:?}", v).as_str(),
            ))),
        }
    }
}

impl SaslMechanisms {
    pub fn decode(mut decoder: FrameDecoder) -> Result<SaslMechanisms> {
        let mut mechs = SaslMechanisms {
            mechanisms: Vec::new(),
        };
        decoder.decode_optional(&mut mechs.mechanisms)?;
        Ok(mechs)
    }
}

impl TryFromValue for SaslMechanism {
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Symbol(mech) => SaslMechanism::from_slice(&mech[..]),
            _ => Err(AmqpError::decode_error(Some(
                "Error converting value to SaslMechanism",
            ))),
        }
    }
}
impl TryFromValueVec for SaslMechanism {}

/** AMQP frame types. */
#[derive(Debug, Clone)]
pub struct AmqpFrame {
    pub channel: u16,
    pub performative: Option<Performative>,
    pub payload: Option<Vec<u8>>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum Performative {
    Open(Open),
    Close(Close),
    Begin(Begin),
    End(End),
    Attach(Attach),
    Detach(Detach),
    Flow(Flow),
    Transfer(Transfer),
    Disposition(Disposition),
}

#[derive(Debug, Clone)]
pub struct Open {
    pub container_id: String,
    pub hostname: Option<String>,
    pub max_frame_size: Option<u32>,
    pub channel_max: Option<u16>,
    pub idle_timeout: Option<u32>,
    pub outgoing_locales: Option<Vec<Symbol>>,
    pub incoming_locales: Option<Vec<Symbol>>,
    pub offered_capabilities: Option<Vec<Symbol>>,
    pub desired_capabilities: Option<Vec<Symbol>>,
    pub properties: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone)]
pub struct Close {
    pub error: Option<ErrorCondition>,
}

#[derive(Debug, Clone)]
pub struct Begin {
    pub remote_channel: Option<u16>,
    pub next_outgoing_id: u32,
    pub incoming_window: u32,
    pub outgoing_window: u32,
    pub handle_max: Option<u32>,
    pub offered_capabilities: Option<Vec<Symbol>>,
    pub desired_capabilities: Option<Vec<Symbol>>,
    pub properties: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone)]
pub struct End {
    pub error: Option<ErrorCondition>,
}

#[derive(Debug, Clone)]
pub struct Attach {
    pub name: String,
    pub handle: u32,
    pub role: LinkRole,
    pub snd_settle_mode: Option<SenderSettleMode>,
    pub rcv_settle_mode: Option<ReceiverSettleMode>,
    pub source: Option<Source>,
    pub target: Option<Target>,
    pub unsettled: Option<BTreeMap<Value, Value>>,
    pub incomplete_unsettled: Option<bool>,
    pub initial_delivery_count: Option<u32>,
    pub max_message_size: Option<u64>,
    pub offered_capabilities: Option<Vec<Symbol>>,
    pub desired_capabilities: Option<Vec<Symbol>>,
    pub properties: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone)]
pub struct Detach {
    pub handle: u32,
    pub closed: Option<bool>,
    pub error: Option<ErrorCondition>,
}

#[derive(Debug, Clone)]
pub struct Flow {
    pub next_incoming_id: Option<u32>,
    pub incoming_window: u32,
    pub next_outgoing_id: u32,
    pub outgoing_window: u32,
    pub handle: Option<u32>,
    pub delivery_count: Option<u32>,
    pub link_credit: Option<u32>,
    pub available: Option<u32>,
    pub drain: Option<bool>,
    pub echo: Option<bool>,
    pub properties: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone)]
pub struct Transfer {
    pub handle: u32,
    pub delivery_id: Option<u32>,
    pub delivery_tag: Option<Vec<u8>>,
    pub message_format: Option<u32>,
    pub settled: Option<bool>,
    pub more: Option<bool>,
    pub rcv_settle_mode: Option<ReceiverSettleMode>,
    pub state: Option<DeliveryState>,
    pub resume: Option<bool>,
    pub aborted: Option<bool>,
    pub batchable: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct Disposition {
    pub role: LinkRole,
    pub first: u32,
    pub last: Option<u32>,
    pub settled: Option<bool>,
    pub state: Option<DeliveryState>,
    pub batchable: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryState {
    Received(Received),
    Accepted,
    Rejected(Rejected),
    Released,
    Modified(Modified),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Received {
    pub section_number: u32,
    pub section_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejected {
    pub error: Option<ErrorCondition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Modified {
    pub delivery_failed: Option<bool>,
    pub undeliverable_here: Option<bool>,
    pub message_annotations: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone)]
pub struct Source {
    pub address: Option<String>,
    pub durable: Option<TerminusDurability>,
    pub expiry_policy: Option<TerminusExpiryPolicy>,
    pub timeout: Option<u32>,
    pub dynamic: Option<bool>,
    pub dynamic_node_properties: Option<BTreeMap<Symbol, Value>>,
    pub distribution_mode: Option<Symbol>,
    pub filter: Option<BTreeMap<Symbol, Value>>,
    pub default_outcome: Option<Outcome>,
    pub outcomes: Option<Vec<Outcome>>,
    pub capabilities: Option<Vec<Symbol>>,
}

#[derive(Debug, Clone)]
pub struct Target {
    pub address: Option<String>,
    pub durable: Option<TerminusDurability>,
    pub expiry_policy: Option<TerminusExpiryPolicy>,
    pub timeout: Option<u32>,
    pub dynamic: Option<bool>,
    pub dynamic_node_properties: Option<BTreeMap<Symbol, Value>>,
    pub capabilities: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum LinkRole {
    Sender,
    Receiver,
}

impl LinkRole {
    pub fn to_string(&self) -> &str {
        match self {
            LinkRole::Sender => "sender",
            LinkRole::Receiver => "received",
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum SenderSettleMode {
    Unsettled,
    Settled,
    Mixed,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum ReceiverSettleMode {
    First,
    Second,
}

#[derive(Debug, Clone, Copy)]
pub enum TerminusDurability {
    None,
    Configuration,
    UnsettledState,
}

#[derive(Debug, Clone, Copy)]
pub enum TerminusExpiryPolicy {
    LinkDetach,
    SessionEnd,
    ConnectionClose,
    Never,
}

#[derive(Debug, Clone, Copy)]
pub enum Outcome {
    Accepted,
    Rejected,
    Released,
    Modified,
}

impl Open {
    pub fn new(container_id: &str) -> Open {
        Open {
            container_id: container_id.to_string(),
            hostname: None,
            max_frame_size: None,
            channel_max: None,
            idle_timeout: None,
            outgoing_locales: None,
            incoming_locales: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }

    pub fn decode(mut decoder: FrameDecoder) -> Result<Open> {
        let mut open = Open {
            container_id: String::new(),
            hostname: None,
            max_frame_size: None,
            channel_max: None,
            idle_timeout: None,
            outgoing_locales: None,
            incoming_locales: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        };
        decoder.decode_required(&mut open.container_id)?;
        decoder.decode_optional(&mut open.hostname)?;
        decoder.decode_optional(&mut open.max_frame_size)?;
        decoder.decode_optional(&mut open.channel_max)?;
        decoder.decode_optional(&mut open.idle_timeout)?;
        decoder.decode_optional(&mut open.outgoing_locales)?;
        decoder.decode_optional(&mut open.incoming_locales)?;
        decoder.decode_optional(&mut open.offered_capabilities)?;
        decoder.decode_optional(&mut open.desired_capabilities)?;
        decoder.decode_optional(&mut open.properties)?;
        Ok(open)
    }
}

impl Encoder for Open {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_OPEN);
        encoder.encode_arg(&self.container_id)?;
        encoder.encode_arg(&self.hostname)?;
        encoder.encode_arg(&self.max_frame_size)?;
        encoder.encode_arg(&self.channel_max)?;
        encoder.encode_arg(&self.idle_timeout)?;
        encoder.encode_arg(&self.outgoing_locales)?;
        encoder.encode_arg(&self.incoming_locales)?;
        encoder.encode_arg(&self.offered_capabilities)?;
        encoder.encode_arg(&self.desired_capabilities)?;
        encoder.encode_arg(&self.properties)?;
        encoder.encode(writer)
    }
}

impl Close {
    pub fn decode(mut decoder: FrameDecoder) -> Result<Close> {
        let mut close = Close { error: None };
        decoder.decode_optional(&mut close.error)?;
        Ok(close)
    }
}

impl Encoder for Close {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_CLOSE);
        encoder.encode_arg(&self.error)?;
        encoder.encode(writer)
    }
}

impl Begin {
    pub fn new(next_outgoing_id: u32, incoming_window: u32, outgoing_window: u32) -> Begin {
        Begin {
            remote_channel: None,
            next_outgoing_id,
            incoming_window,
            outgoing_window,
            handle_max: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }

    pub fn decode(mut decoder: FrameDecoder) -> Result<Begin> {
        let mut begin = Begin::new(0, 0, 0);
        decoder.decode_optional(&mut begin.remote_channel)?;
        decoder.decode_required(&mut begin.next_outgoing_id)?;
        decoder.decode_required(&mut begin.incoming_window)?;
        decoder.decode_required(&mut begin.outgoing_window)?;
        decoder.decode_optional(&mut begin.handle_max)?;
        decoder.decode_optional(&mut begin.offered_capabilities)?;
        decoder.decode_optional(&mut begin.desired_capabilities)?;
        decoder.decode_optional(&mut begin.properties)?;
        Ok(begin)
    }
}

impl Encoder for Begin {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_BEGIN);
        encoder.encode_arg(&self.remote_channel)?;
        encoder.encode_arg(&self.next_outgoing_id)?;
        encoder.encode_arg(&self.incoming_window)?;
        encoder.encode_arg(&self.outgoing_window)?;
        encoder.encode_arg(&self.handle_max)?;
        encoder.encode_arg(&self.offered_capabilities)?;
        encoder.encode_arg(&self.desired_capabilities)?;
        encoder.encode_arg(&self.properties)?;
        encoder.encode(writer)
    }
}

impl End {
    pub fn decode(mut decoder: FrameDecoder) -> Result<End> {
        let mut end = End { error: None };
        decoder.decode_optional(&mut end.error)?;
        Ok(end)
    }
}

impl Encoder for End {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_END);
        encoder.encode_arg(&self.error)?;
        encoder.encode(writer)
    }
}

impl Attach {
    pub fn new(name: &str, handle: u32, role: LinkRole) -> Attach {
        Attach {
            name: name.to_string(),
            handle,
            role,
            snd_settle_mode: None,
            rcv_settle_mode: None,
            source: None,
            target: None,
            unsettled: None,
            incomplete_unsettled: None,
            initial_delivery_count: None,
            max_message_size: None,
            offered_capabilities: None,
            desired_capabilities: None,
            properties: None,
        }
    }

    pub fn source(mut self, source: Source) -> Self {
        self.source = Some(source);
        self
    }

    pub fn target(mut self, target: Target) -> Self {
        self.target = Some(target);
        self
    }

    pub fn initial_delivery_count(mut self, initial_delivery_count: u32) -> Self {
        self.initial_delivery_count = Some(initial_delivery_count);
        self
    }

    pub fn decode(mut decoder: FrameDecoder) -> Result<Attach> {
        let mut attach = Attach::new("", 0, LinkRole::Sender);
        decoder.decode_required(&mut attach.name)?;
        decoder.decode_required(&mut attach.handle)?;
        decoder.decode_required(&mut attach.role)?;
        decoder.decode_optional(&mut attach.snd_settle_mode)?;
        decoder.decode_optional(&mut attach.rcv_settle_mode)?;
        decoder.decode_optional(&mut attach.source)?;
        decoder.decode_optional(&mut attach.target)?;
        decoder.decode_optional(&mut attach.unsettled)?;
        decoder.decode_optional(&mut attach.incomplete_unsettled)?;
        decoder.decode_optional(&mut attach.initial_delivery_count)?;
        decoder.decode_optional(&mut attach.max_message_size)?;
        decoder.decode_optional(&mut attach.offered_capabilities)?;
        decoder.decode_optional(&mut attach.desired_capabilities)?;
        decoder.decode_optional(&mut attach.properties)?;
        Ok(attach)
    }
}

impl Encoder for Attach {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_ATTACH);
        encoder.encode_arg(&self.name)?;
        encoder.encode_arg(&self.handle)?;
        encoder.encode_arg(&self.role)?;
        encoder.encode_arg(&self.snd_settle_mode.unwrap_or(SenderSettleMode::Mixed))?;
        encoder.encode_arg(&self.rcv_settle_mode.unwrap_or(ReceiverSettleMode::First))?;
        encoder.encode_arg(&self.source)?;
        encoder.encode_arg(&self.target)?;
        encoder.encode_arg(&self.unsettled)?;
        encoder.encode_arg(&self.incomplete_unsettled.unwrap_or(false))?;
        encoder.encode_arg(&self.initial_delivery_count)?;
        encoder.encode_arg(&self.max_message_size)?;
        encoder.encode_arg(&self.offered_capabilities)?;
        encoder.encode_arg(&self.desired_capabilities)?;
        encoder.encode_arg(&self.properties)?;
        encoder.encode(writer)
    }
}

impl Detach {
    pub fn decode(mut decoder: FrameDecoder) -> Result<Detach> {
        let mut detach = Detach {
            handle: 0,
            closed: None,
            error: None,
        };
        decoder.decode_required(&mut detach.handle)?;
        decoder.decode_optional(&mut detach.closed)?;
        decoder.decode_optional(&mut detach.error)?;
        Ok(detach)
    }
}

impl Encoder for Detach {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_DETACH);
        encoder.encode_arg(&self.handle)?;
        encoder.encode_arg(&self.closed)?;
        encoder.encode_arg(&self.error)?;
        encoder.encode(writer)
    }
}

impl Flow {
    pub fn decode(mut decoder: FrameDecoder) -> Result<Flow> {
        let mut flow = Flow {
            next_incoming_id: None,
            incoming_window: 0,
            next_outgoing_id: 0,
            outgoing_window: 0,
            handle: None,
            delivery_count: None,
            link_credit: None,
            available: None,
            drain: Some(false),
            echo: Some(false),
            properties: None,
        };
        decoder.decode_optional(&mut flow.next_incoming_id)?;
        decoder.decode_required(&mut flow.incoming_window)?;
        decoder.decode_required(&mut flow.next_outgoing_id)?;
        decoder.decode_required(&mut flow.outgoing_window)?;
        decoder.decode_optional(&mut flow.handle)?;
        decoder.decode_optional(&mut flow.delivery_count)?;
        decoder.decode_optional(&mut flow.link_credit)?;
        decoder.decode_optional(&mut flow.available)?;
        decoder.decode_optional(&mut flow.drain)?;
        decoder.decode_optional(&mut flow.echo)?;
        decoder.decode_optional(&mut flow.properties)?;
        Ok(flow)
    }
}

impl Encoder for Flow {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_FLOW);
        encoder.encode_arg(&self.next_incoming_id)?;
        encoder.encode_arg(&self.incoming_window)?;
        encoder.encode_arg(&self.next_outgoing_id)?;
        encoder.encode_arg(&self.outgoing_window)?;
        encoder.encode_arg(&self.handle)?;
        encoder.encode_arg(&self.delivery_count)?;
        encoder.encode_arg(&self.link_credit)?;
        encoder.encode_arg(&self.available)?;
        encoder.encode_arg(&self.drain)?;
        encoder.encode_arg(&self.echo)?;
        encoder.encode_arg(&self.properties)?;
        encoder.encode(writer)
    }
}

impl Transfer {
    pub fn new(handle: u32) -> Transfer {
        Transfer {
            handle,
            delivery_id: None,
            delivery_tag: None,
            message_format: None,
            settled: None,
            more: Some(false),
            rcv_settle_mode: None,
            state: None,
            resume: Some(false),
            aborted: Some(false),
            batchable: Some(false),
        }
    }

    pub fn delivery_id(mut self, delivery_id: u32) -> Self {
        self.delivery_id = Some(delivery_id);
        self
    }

    pub fn delivery_tag(mut self, delivery_tag: &[u8]) -> Self {
        self.delivery_tag = Some(delivery_tag.to_vec());
        self
    }

    pub fn settled(mut self, settled: bool) -> Self {
        self.settled = Some(settled);
        self
    }

    pub fn decode(mut decoder: FrameDecoder) -> Result<Transfer> {
        let mut transfer = Transfer::new(0);
        decoder.decode_required(&mut transfer.handle)?;
        decoder.decode_optional(&mut transfer.delivery_id)?;
        decoder.decode_optional(&mut transfer.delivery_tag)?;
        decoder.decode_optional(&mut transfer.message_format)?;
        decoder.decode_optional(&mut transfer.settled)?;
        decoder.decode_optional(&mut transfer.more)?;
        decoder.decode_optional(&mut transfer.rcv_settle_mode)?;
        decoder.decode_optional(&mut transfer.state)?;
        decoder.decode_optional(&mut transfer.resume)?;
        decoder.decode_optional(&mut transfer.aborted)?;
        decoder.decode_optional(&mut transfer.batchable)?;
        Ok(transfer)
    }
}

impl Encoder for Transfer {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_TRANSFER);
        encoder.encode_arg(&self.handle)?;
        encoder.encode_arg(&self.delivery_id)?;
        encoder.encode_arg(&self.delivery_tag)?;
        encoder.encode_arg(&self.message_format)?;
        encoder.encode_arg(&self.settled)?;
        encoder.encode_arg(&self.more)?;
        encoder.encode_arg(&self.rcv_settle_mode)?;
        encoder.encode_arg(&self.state)?;
        encoder.encode_arg(&self.resume)?;
        encoder.encode_arg(&self.aborted)?;
        encoder.encode_arg(&self.batchable)?;
        encoder.encode(writer)
    }
}

impl Disposition {
    pub fn decode(mut decoder: FrameDecoder) -> Result<Disposition> {
        let mut disposition = Disposition {
            role: LinkRole::Sender,
            first: 0,
            last: None,
            settled: Some(false),
            state: None,
            batchable: Some(false),
        };
        decoder.decode_required(&mut disposition.role)?;
        decoder.decode_required(&mut disposition.first)?;
        decoder.decode_optional(&mut disposition.last)?;
        decoder.decode_optional(&mut disposition.settled)?;
        decoder.decode_optional(&mut disposition.state)?;
        decoder.decode_optional(&mut disposition.batchable)?;
        Ok(disposition)
    }
}

impl Encoder for Disposition {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_DISPOSITION);
        encoder.encode_arg(&self.role)?;
        encoder.encode_arg(&self.first)?;
        encoder.encode_arg(&self.last)?;
        encoder.encode_arg(&self.settled)?;
        encoder.encode_arg(&self.state)?;
        encoder.encode_arg(&self.batchable)?;
        encoder.encode(writer)
    }
}

impl Source {
    pub fn decode(mut decoder: FrameDecoder) -> Result<Source> {
        let mut source = Source {
            address: None,
            durable: None,
            expiry_policy: None,
            timeout: None,
            dynamic: None,
            dynamic_node_properties: None,
            distribution_mode: None,
            filter: None,
            default_outcome: None,
            outcomes: None,
            capabilities: None,
        };
        decoder.decode_optional(&mut source.address)?;
        decoder.decode_optional(&mut source.durable)?;
        decoder.decode_optional(&mut source.expiry_policy)?;
        decoder.decode_optional(&mut source.timeout)?;
        decoder.decode_optional(&mut source.dynamic)?;
        decoder.decode_optional(&mut source.dynamic_node_properties)?;
        decoder.decode_optional(&mut source.distribution_mode)?;
        decoder.decode_optional(&mut source.filter)?;
        decoder.decode_optional(&mut source.default_outcome)?;
        decoder.decode_optional(&mut source.outcomes)?;
        decoder.decode_optional(&mut source.capabilities)?;
        Ok(source)
    }
}

impl Encoder for Source {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_SOURCE);
        encoder.encode_arg(&self.address)?;
        encoder.encode_arg(&self.durable.unwrap_or(TerminusDurability::None))?;
        encoder.encode_arg(
            &self
                .expiry_policy
                .unwrap_or(TerminusExpiryPolicy::SessionEnd),
        )?;
        encoder.encode_arg(&self.timeout.unwrap_or(0))?;
        encoder.encode_arg(&self.dynamic.unwrap_or(false))?;
        encoder.encode_arg(&self.dynamic_node_properties)?;
        encoder.encode_arg(&self.distribution_mode)?;
        encoder.encode_arg(&self.filter)?;
        encoder.encode_arg(&self.default_outcome)?;
        encoder.encode_arg(&self.outcomes)?;
        encoder.encode_arg(&self.capabilities)?;
        encoder.encode(writer)
    }
}

impl Target {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Target {
        Target {
            address: None,
            durable: None,
            expiry_policy: None,
            timeout: None,
            dynamic: None,
            dynamic_node_properties: None,
            capabilities: None,
        }
    }

    pub fn address(mut self, address: &str) -> Self {
        self.address = Some(address.to_string());
        self
    }

    pub fn decode(mut decoder: FrameDecoder) -> Result<Target> {
        let mut target = Target::new();
        decoder.decode_optional(&mut target.address)?;
        decoder.decode_optional(&mut target.durable)?;
        decoder.decode_optional(&mut target.expiry_policy)?;
        decoder.decode_optional(&mut target.timeout)?;
        decoder.decode_optional(&mut target.dynamic)?;
        decoder.decode_optional(&mut target.dynamic_node_properties)?;
        decoder.decode_optional(&mut target.capabilities)?;
        Ok(target)
    }
}

impl Encoder for Target {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut encoder = FrameEncoder::new(DESC_TARGET);
        encoder.encode_arg(&self.address)?;
        encoder.encode_arg(&self.durable.unwrap_or(TerminusDurability::None))?;
        encoder.encode_arg(
            &self
                .expiry_policy
                .unwrap_or(TerminusExpiryPolicy::SessionEnd),
        )?;
        encoder.encode_arg(&self.timeout.unwrap_or(0))?;
        encoder.encode_arg(&self.dynamic.unwrap_or(false))?;
        encoder.encode_arg(&self.dynamic_node_properties)?;
        encoder.encode_arg(&self.capabilities)?;
        encoder.encode(writer)
    }
}

impl Encoder for DeliveryState {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let value = match self {
            DeliveryState::Received(received) => {
                let mut encoder = FrameEncoder::new(DESC_DELIVERY_STATE_RECEIVED);
                encoder.encode_arg(&received.section_number)?;
                encoder.encode_arg(&received.section_offset)?;
                encoder
            }
            DeliveryState::Accepted => FrameEncoder::new(DESC_DELIVERY_STATE_ACCEPTED),
            DeliveryState::Rejected(rejected) => {
                let mut encoder = FrameEncoder::new(DESC_DELIVERY_STATE_REJECTED);
                encoder.encode_arg(&rejected.error)?;
                encoder
            }
            DeliveryState::Released => FrameEncoder::new(DESC_DELIVERY_STATE_RELEASED),
            DeliveryState::Modified(modified) => {
                let mut encoder = FrameEncoder::new(DESC_DELIVERY_STATE_MODIFIED);
                encoder.encode_arg(&modified.delivery_failed)?;
                encoder.encode_arg(&modified.undeliverable_here)?;
                encoder.encode_arg(&modified.message_annotations)?;
                encoder
            }
        };
        value.encode(writer)
    }
}

impl TryFromValue for DeliveryState {
    fn try_from(value: Value) -> Result<Self> {
        if let Value::Described(descriptor, mut body) = value {
            let mut decoder = FrameDecoder::new(&descriptor, &mut body)?;
            match *descriptor {
                DESC_DELIVERY_STATE_RECEIVED => {
                    let mut received = Received {
                        section_number: 0,
                        section_offset: 0,
                    };
                    decoder.decode_required(&mut received.section_number)?;
                    decoder.decode_required(&mut received.section_offset)?;
                    Ok(DeliveryState::Received(received))
                }
                DESC_DELIVERY_STATE_ACCEPTED => Ok(DeliveryState::Accepted),
                DESC_DELIVERY_STATE_REJECTED => {
                    let mut rejected = Rejected { error: None };
                    decoder.decode_optional(&mut rejected.error)?;
                    Ok(DeliveryState::Rejected(rejected))
                }
                DESC_DELIVERY_STATE_RELEASED => Ok(DeliveryState::Released),
                DESC_DELIVERY_STATE_MODIFIED => {
                    let mut modified = Modified {
                        delivery_failed: None,
                        undeliverable_here: None,
                        message_annotations: None,
                    };
                    decoder.decode_optional(&mut modified.delivery_failed)?;
                    decoder.decode_optional(&mut modified.undeliverable_here)?;
                    decoder.decode_optional(&mut modified.message_annotations)?;
                    Ok(DeliveryState::Modified(modified))
                }
                _ => Err(AmqpError::decode_error(Some(
                    "Error converting value to DeliveryState",
                ))),
            }
        } else {
            Err(AmqpError::decode_error(Some(
                "Error converting value to DeliveryState",
            )))
        }
    }
}

impl TerminusDurability {
    pub fn from_int(input: u32) -> Result<TerminusDurability> {
        match input {
            0 => Ok(TerminusDurability::None),
            1 => Ok(TerminusDurability::Configuration),
            2 => Ok(TerminusDurability::UnsettledState),
            v => Err(AmqpError::decode_error(Some(
                format!("Unknown terminus durability{:?}", v).as_str(),
            ))),
        }
    }
}

impl TryFromValue for TerminusDurability {
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Uint(v) => TerminusDurability::from_int(v),
            _ => Err(AmqpError::decode_error(Some(
                "Error converting value to TerminusDurability",
            ))),
        }
    }
}

impl TerminusExpiryPolicy {
    pub fn from_slice(data: &[u8]) -> Result<TerminusExpiryPolicy> {
        let input = std::str::from_utf8(data)?;
        match input {
            "link-detach" => Ok(TerminusExpiryPolicy::LinkDetach),
            "session-end" => Ok(TerminusExpiryPolicy::SessionEnd),
            "connection-close" => Ok(TerminusExpiryPolicy::ConnectionClose),
            "never" => Ok(TerminusExpiryPolicy::Never),
            v => Err(AmqpError::decode_error(Some(
                format!("Unknown terminus expiry policy {:?}", v).as_str(),
            ))),
        }
    }
}

impl TryFromValue for TerminusExpiryPolicy {
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Symbol(v) => TerminusExpiryPolicy::from_slice(&v[..]),
            _ => Err(AmqpError::decode_error(Some(
                "Error converting value to TerminusExpiryPolicy",
            ))),
        }
    }
}

impl Outcome {
    pub fn from_slice(data: &[u8]) -> Result<Outcome> {
        let input = std::str::from_utf8(data)?;
        match input {
            "amqp:accepted:list" => Ok(Outcome::Accepted),
            "amqp:rejected:list" => Ok(Outcome::Rejected),
            "amqp:released:list" => Ok(Outcome::Released),
            "amqp:modified:list" => Ok(Outcome::Modified),
            v => Err(AmqpError::decode_error(Some(
                format!("Unknown outcome {:?}", v).as_str(),
            ))),
        }
    }
}

impl TryFromValueVec for Outcome {}
impl TryFromValue for Outcome {
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Symbol(v) => Outcome::from_slice(&v[..]),
            Value::Described(desc, _) => match *desc {
                DESC_DELIVERY_STATE_ACCEPTED => Ok(Outcome::Accepted),
                DESC_DELIVERY_STATE_MODIFIED => Ok(Outcome::Modified),
                DESC_DELIVERY_STATE_RELEASED => Ok(Outcome::Released),
                DESC_DELIVERY_STATE_REJECTED => Ok(Outcome::Rejected),
                _ => Err(AmqpError::decode_error(Some("Unknown outcome descriptor"))),
            },
            _ => Err(AmqpError::decode_error(Some(
                format!("Error converting value to Outcome. Was {:?}", value).as_str(),
            ))),
        }
    }
}

impl TryFromValue for LinkRole {
    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::Bool(value) => Ok(if value {
                LinkRole::Sender
            } else {
                LinkRole::Receiver
            }),
            _ => Err(AmqpError::decode_error(Some(
                "Error converting value to LinkRole",
            ))),
        }
    }
}

impl TryFromValue for SenderSettleMode {
    fn try_from(value: Value) -> Result<Self> {
        if let Value::Ubyte(value) = value {
            Ok(match value {
                0 => SenderSettleMode::Unsettled,
                1 => SenderSettleMode::Settled,
                _ => SenderSettleMode::Mixed,
            })
        } else {
            Err(AmqpError::decode_error(Some(
                "Error converting value to SenderSettleMode",
            )))
        }
    }
}

impl TryFromValue for Source {
    fn try_from(value: Value) -> Result<Self> {
        if let Value::Described(descriptor, mut body) = value {
            let decoder = FrameDecoder::new(&descriptor, &mut body)?;
            Ok(Source::decode(decoder)?)
        } else {
            Err(AmqpError::decode_error(Some(
                "Error converting value to ReceiverSettleMode",
            )))
        }
    }
}

impl TryFromValue for Target {
    fn try_from(value: Value) -> Result<Self> {
        if let Value::Described(descriptor, mut body) = value {
            let decoder = FrameDecoder::new(&descriptor, &mut body)?;
            Ok(Target::decode(decoder)?)
        } else {
            Err(AmqpError::decode_error(Some(
                "Error converting value to ReceiverSettleMode",
            )))
        }
    }
}

impl TryFromValue for ReceiverSettleMode {
    fn try_from(value: Value) -> Result<Self> {
        if let Value::Ubyte(value) = value {
            match value {
                0 => Ok(ReceiverSettleMode::First),
                1 => Ok(ReceiverSettleMode::Second),
                _ => Err(AmqpError::decode_error(Some(
                    "Error converting value to ReceiverSettledMode",
                ))),
            }
        } else {
            Err(AmqpError::decode_error(Some(
                "Error converting value to ReceiverSettleMode",
            )))
        }
    }
}

impl Encoder for SenderSettleMode {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::Ubyte(&(*self as u8)).encode(writer)
    }
}

impl Encoder for ReceiverSettleMode {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        Value::Ubyte(*self as u8).encode(writer)
    }
}

impl Encoder for LinkRole {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        Value::Bool(*self == LinkRole::Receiver).encode(writer)
    }
}

impl Encoder for Vec<Outcome> {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        let mut values = Vec::new();
        for outcome in self.iter() {
            values.push(ValueRef::SymbolRef(outcome.to_str()));
        }
        ValueRef::ArrayRef(&values).encode(writer)
    }
}

impl Outcome {
    pub fn to_str(&self) -> &'static str {
        match *self {
            Outcome::Accepted => "Accepted",
            Outcome::Rejected => "Rejected",
            Outcome::Released => "Released",
            Outcome::Modified => "Modified",
        }
    }
}
impl Encoder for Outcome {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::SymbolRef(self.to_str()).encode(writer)
    }
}

impl TerminusExpiryPolicy {
    pub fn to_str(&self) -> &'static str {
        match *self {
            TerminusExpiryPolicy::LinkDetach => "link-detach",
            TerminusExpiryPolicy::SessionEnd => "session-end",
            TerminusExpiryPolicy::ConnectionClose => "connection-close",
            TerminusExpiryPolicy::Never => "never",
        }
    }
}

impl Encoder for TerminusDurability {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        Value::Uint(match self {
            TerminusDurability::None => 0,
            TerminusDurability::Configuration => 1,
            TerminusDurability::UnsettledState => 2,
        })
        .encode(writer)
    }
}

impl Encoder for TerminusExpiryPolicy {
    fn encode(&self, writer: &mut dyn Write) -> Result<TypeCode> {
        ValueRef::SymbolRef(self.to_str()).encode(writer)
    }
}

impl FrameHeader {
    pub fn decode(reader: &mut dyn Read) -> Result<FrameHeader> {
        Ok(FrameHeader {
            size: reader.read_u32::<NetworkEndian>()?,
            doff: reader.read_u8()?,
            frame_type: reader.read_u8()?,
            ext: reader.read_u16::<NetworkEndian>()?,
        })
    }

    pub fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        writer.write_u32::<NetworkEndian>(self.size)?;
        writer.write_u8(self.doff)?;
        writer.write_u8(self.frame_type)?;
        writer.write_u16::<NetworkEndian>(self.ext)?;
        Ok(())
    }
}

impl Frame {
    pub fn encode(&self, writer: &mut dyn Write) -> Result<usize> {
        let mut header: FrameHeader = FrameHeader {
            size: 8,
            doff: 2,
            frame_type: 0,
            ext: 0,
        };

        let mut buf: Vec<u8> = Vec::new();

        match self {
            Frame::AMQP(AmqpFrame {
                channel,
                performative,
                payload,
            }) => {
                header.frame_type = 0;
                header.ext = *channel;

                if let Some(performative) = performative {
                    match performative {
                        Performative::Open(open) => {
                            open.encode(&mut buf)?;
                        }
                        Performative::Begin(begin) => {
                            begin.encode(&mut buf)?;
                        }
                        Performative::Attach(attach) => {
                            attach.encode(&mut buf)?;
                        }
                        Performative::Detach(detach) => {
                            detach.encode(&mut buf)?;
                        }
                        Performative::End(end) => {
                            end.encode(&mut buf)?;
                        }
                        Performative::Close(close) => {
                            close.encode(&mut buf)?;
                        }
                        Performative::Flow(flow) => {
                            flow.encode(&mut buf)?;
                        }
                        Performative::Transfer(transfer) => {
                            transfer.encode(&mut buf)?;
                        }
                        Performative::Disposition(disposition) => {
                            disposition.encode(&mut buf)?;
                        }
                    }
                }

                if let Some(data) = payload {
                    buf.write_all(&data[..])?;
                }
            }
            Frame::SASL(sasl_frame) => {
                header.frame_type = 1;
                match sasl_frame {
                    SaslFrame::SaslMechanisms(_) => {}
                    SaslFrame::SaslInit(init) => {
                        init.encode(&mut buf)?;
                    }
                    SaslFrame::SaslChallenge(_) => {}
                    SaslFrame::SaslResponse(_) => {}
                    SaslFrame::SaslOutcome(_) => {}
                }
            }
        }

        header.size += buf.len() as u32;

        header.encode(writer)?;
        writer.write_all(&buf[..])?;

        Ok(header.size as usize)
    }

    pub fn decode(header: FrameHeader, reader: &mut Cursor<&mut &[u8]>) -> Result<Frame> {
        // Read off extended header not in use
        let mut doff = header.doff;
        while doff > 2 {
            reader.read_u32::<NetworkEndian>()?;
            doff -= 1;
        }

        if header.frame_type == 0 {
            let performative = if header.size > 8 {
                if let Value::Described(descriptor, mut value) = decode_value(reader)? {
                    let decoder = FrameDecoder::new(&descriptor, &mut value)?;
                    Some(match *descriptor {
                        DESC_OPEN => {
                            let open = Open::decode(decoder)?;
                            Ok(Performative::Open(open))
                        }
                        DESC_CLOSE => {
                            let close = Close::decode(decoder)?;
                            Ok(Performative::Close(close))
                        }
                        DESC_BEGIN => {
                            let begin = Begin::decode(decoder)?;
                            Ok(Performative::Begin(begin))
                        }
                        DESC_END => {
                            let end = End::decode(decoder)?;
                            Ok(Performative::End(end))
                        }
                        DESC_ATTACH => {
                            let attach = Attach::decode(decoder)?;
                            Ok(Performative::Attach(attach))
                        }
                        DESC_DETACH => {
                            let detach = Detach::decode(decoder)?;
                            Ok(Performative::Detach(detach))
                        }
                        DESC_FLOW => {
                            let flow = Flow::decode(decoder)?;
                            Ok(Performative::Flow(flow))
                        }
                        DESC_TRANSFER => {
                            let transfer = Transfer::decode(decoder)?;
                            Ok(Performative::Transfer(transfer))
                        }
                        DESC_DISPOSITION => {
                            let disposition = Disposition::decode(decoder)?;
                            Ok(Performative::Disposition(disposition))
                        }
                        v => Err(AmqpError::decode_error(Some(
                            format!("Unexpected descriptor value: {:?}", v).as_str(),
                        ))),
                    }?)
                } else {
                    None
                }
            } else {
                None
            };

            // Figure out how much data we have left in the frame
            let total_payload_size = header.size - ((header.doff as u32) * 4);
            let payload_position = reader.position() as u32;
            let payload_size = total_payload_size - payload_position;

            let mut payload = vec![0; payload_size as usize];
            reader.read_exact(&mut payload[..])?;

            Ok(Frame::AMQP(AmqpFrame {
                channel: header.ext,
                performative,
                payload: Some(payload),
            }))
        } else if header.frame_type == 1 {
            if header.size > 8 {
                if let Value::Described(descriptor, mut value) = decode_value(reader)? {
                    let decoder = FrameDecoder::new(&descriptor, &mut value)?;
                    let frame = match *descriptor {
                        DESC_SASL_MECHANISMS => {
                            Some(SaslFrame::SaslMechanisms(SaslMechanisms::decode(decoder)?))
                        }
                        DESC_SASL_OUTCOME => {
                            Some(SaslFrame::SaslOutcome(SaslOutcome::decode(decoder)?))
                        }
                        _ => None,
                    };

                    match frame {
                        Some(frame) => Ok(Frame::SASL(frame)),
                        None => Err(AmqpError::decode_error(Some("Error decoding sasl frame"))),
                    }
                } else {
                    Err(AmqpError::amqp_error(
                        condition::connection::FRAMING_ERROR,
                        Some("Sasl frame not matched"),
                    ))
                }
            } else {
                Err(AmqpError::amqp_error(
                    condition::connection::FRAMING_ERROR,
                    Some("Sasl frame not matched"),
                ))
            }
        } else {
            Err(AmqpError::amqp_error(
                condition::connection::FRAMING_ERROR,
                Some(format!("Unknown frame type {}", header.frame_type).as_str()),
            ))
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn check_performatives() {
        let frm = Open::new("1234");

        assert_eq!(None, frm.hostname);
        assert_eq!(4, frm.container_id.len());
        assert_eq!(None, frm.max_frame_size);
        assert_eq!(None, frm.channel_max);
    }
}
