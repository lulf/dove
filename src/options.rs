use crate::framing::{Attach, LinkRole};

pub trait ApplyOptionsTo<T> {
    fn apply_options_to(&self, target: &mut T);
}

pub enum LinkOptions {
    Sender(Option<SenderOptions>),
    Receiver(Option<ReceiverOptions>),
}

impl From<SenderOptions> for LinkOptions {
    fn from(options: SenderOptions) -> Self {
        LinkOptions::Sender(Some(options))
    }
}

impl From<ReceiverOptions> for LinkOptions {
    fn from(options: ReceiverOptions) -> Self {
        LinkOptions::Receiver(Some(options))
    }
}

impl From<LinkRole> for LinkOptions {
    fn from(role: LinkRole) -> Self {
        match role {
            LinkRole::Sender => LinkOptions::Sender(None),
            LinkRole::Receiver => LinkOptions::Receiver(None),
        }
    }
}

impl LinkOptions {
    pub fn role(&self) -> LinkRole {
        match self {
            LinkOptions::Sender(_) => LinkRole::Sender,
            LinkOptions::Receiver(_) => LinkRole::Receiver,
        }
    }

    pub fn applied_on_attach(&self, mut attach: Attach) -> Attach {
        self.apply_options_to(&mut attach);
        attach
    }
}

impl<T, O: ApplyOptionsTo<T>> ApplyOptionsTo<T> for Option<O> {
    fn apply_options_to(&self, target: &mut T) {
        if let Some(inner) = self.as_ref() {
            inner.apply_options_to(target);
        }
    }
}
impl ApplyOptionsTo<Attach> for LinkOptions {
    fn apply_options_to(&self, target: &mut Attach) {
        match self {
            LinkOptions::Sender(sender) => sender.apply_options_to(target),
            LinkOptions::Receiver(receiver) => receiver.apply_options_to(target),
        }
    }
}

pub struct SenderOptions {}

impl ApplyOptionsTo<Attach> for SenderOptions {
    fn apply_options_to(&self, target: &mut Attach) {
        let _ = target;
    }
}

#[derive(Default)]
pub struct ReceiverOptions {
    pub filter: Option<ReceiverFilter>,
}

#[allow(clippy::needless_update)]
impl From<ReceiverFilter> for ReceiverOptions {
    fn from(filter: ReceiverFilter) -> Self {
        Self {
            filter: Some(filter),
            ..Default::default()
        }
    }
}

impl ApplyOptionsTo<Attach> for ReceiverOptions {
    fn apply_options_to(&self, target: &mut Attach) {
        self.filter.apply_options_to(target);
    }
}

/// Sets the filter to be applied by the broker before sending the message
/// through the link. Be aware, that some filters do nothing unless the correct
/// exchange type is set.
pub enum ReceiverFilter {
    /// Filters for exact matches on `message.properties.subject`.
    /// See 'apache.org:legacy-amqp-direct-binding:string'
    /// # WARN
    /// Requires exchange type 'direct'
    ApacheLegacyExchangeDirectBinding(apache_legacy_exchange_direct_binding::Options),

    /// Filters for pattern matches on `message.properties.subject`.
    /// See 'apache.org:legacy-amqp-topic-binding:string'
    /// # WARN
    /// Requires exchange type 'topic'
    ApacheLegacyExchangeTopicBinding(apache_legacy_exchange_topic_binding::Options),

    /// Complex filter based on values set in `message.application_properties.*`.
    /// See 'apache.org:legacy-amqp-headers-binding:map'
    ///
    /// # WARN
    /// Requires exchange type 'headers'
    ApacheLegacyExchangeHeadersBinding(apache_legacy_exchange_headers_filter::Options),
}

impl ApplyOptionsTo<Attach> for ReceiverFilter {
    fn apply_options_to(&self, target: &mut Attach) {
        match self {
            ReceiverFilter::ApacheLegacyExchangeDirectBinding(filter) => {
                filter.apply_options_to(target)
            }
            ReceiverFilter::ApacheLegacyExchangeTopicBinding(filter) => {
                filter.apply_options_to(target)
            }
            ReceiverFilter::ApacheLegacyExchangeHeadersBinding(filter) => {
                filter.apply_options_to(target)
            }
        }
    }
}

impl ReceiverFilter {
    /// Filters for exact matches on `message.properties.subject`.
    /// See 'apache.org:legacy-amqp-direct-binding:string'
    ///
    /// # WARN
    /// Requires exchange type 'direct'
    pub fn apache_legacy_exchange_direct_binding(value: impl Into<String>) -> Self {
        Self::ApacheLegacyExchangeDirectBinding(
            apache_legacy_exchange_direct_binding::Options::from(value),
        )
    }

    /// Filters for pattern matches on `message.properties.subject`.
    /// See 'apache.org:legacy-amqp-topic-binding:string'
    ///
    /// # WARN
    /// Requires exchange type 'topic'
    pub fn apache_legacy_exchange_topic_binding(pattern: impl Into<String>) -> Self {
        Self::ApacheLegacyExchangeTopicBinding(apache_legacy_exchange_topic_binding::Options::from(
            pattern,
        ))
    }

    /// Complex filter based on values set in `message.application_properties.*`.
    /// See 'apache.org:legacy-amqp-headers-binding:map'
    ///
    /// # WARN
    /// Requires exchange type 'headers'
    pub fn apache_legacy_exchange_headers_binding_match_any<A: Into<String>, B: Into<String>>(
        kv_pairs: impl Iterator<Item = (A, B)>,
    ) -> Self {
        Self::apache_legacy_exchange_headers_binding(
            apache_legacy_exchange_headers_filter::MatchMode::Any,
            kv_pairs,
        )
    }

    /// Complex filter based on values set in `message.application_properties.*`.
    /// See 'apache.org:legacy-amqp-headers-binding:map'
    ///
    /// # WARN
    /// Requires exchange type 'headers'
    pub fn apache_legacy_exchange_headers_binding_match_all<A: Into<String>, B: Into<String>>(
        kv_pairs: impl Iterator<Item = (A, B)>,
    ) -> Self {
        Self::apache_legacy_exchange_headers_binding(
            apache_legacy_exchange_headers_filter::MatchMode::All,
            kv_pairs,
        )
    }

    /// Complex filter based on values set in `message.application_properties.*`.
    /// See 'apache.org:legacy-amqp-headers-binding:map'
    ///
    /// # WARN
    /// Requires exchange type 'headers'
    fn apache_legacy_exchange_headers_binding<A: Into<String>, B: Into<String>>(
        mode: apache_legacy_exchange_headers_filter::MatchMode,
        kv_pairs: impl Iterator<Item = (A, B)>,
    ) -> Self {
        Self::ApacheLegacyExchangeHeadersBinding(apache_legacy_exchange_headers_filter::Options {
            mode,
            headers: kv_pairs
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        })
    }
}

pub mod apache_legacy_exchange_direct_binding {
    use super::*;
    use crate::symbol::Symbol;
    use crate::types::Value;
    use std::collections::{BTreeMap, HashMap};

    pub struct Options(String);

    impl<T: Into<String>> From<T> for Options {
        fn from(value: T) -> Self {
            Self(value.into())
        }
    }

    impl ApplyOptionsTo<Attach> for Options {
        fn apply_options_to(&self, target: &mut Attach) {
            if let Some(source) = target.source.as_mut() {
                source.filter = Some({
                    let filter_symbol = "apache.org:legacy-amqp-direct-binding:string";
                    let mut map = BTreeMap::new();
                    map.insert(
                        Symbol::from_string(filter_symbol),
                        Value::Described(
                            Box::new(Value::Symbol(
                                Symbol::from_string(filter_symbol).to_slice().to_vec(),
                            )),
                            Box::new(Value::String(self.0.clone())),
                        ),
                    );
                    map
                });
            }
        }
    }
}

pub mod apache_legacy_exchange_topic_binding {
    use super::*;
    use crate::symbol::Symbol;
    use crate::types::Value;
    use std::collections::{BTreeMap, HashMap};

    pub struct Options(String);

    impl<T: Into<String>> From<T> for Options {
        fn from(value: T) -> Self {
            Self(value.into())
        }
    }

    impl ApplyOptionsTo<Attach> for Options {
        fn apply_options_to(&self, target: &mut Attach) {
            if let Some(source) = target.source.as_mut() {
                source.filter = Some({
                    let filter_symbol = "apache.org:legacy-amqp-topic-binding:string";
                    let mut map = BTreeMap::new();
                    map.insert(
                        Symbol::from_string(filter_symbol),
                        Value::Described(
                            Box::new(Value::Symbol(
                                Symbol::from_string(filter_symbol).to_slice().to_vec(),
                            )),
                            Box::new(Value::String(self.0.clone())),
                        ),
                    );
                    map
                });
            }
        }
    }
}

pub mod apache_legacy_exchange_headers_filter {
    use super::*;
    use crate::symbol::Symbol;
    use crate::types::Value;
    use std::collections::{BTreeMap, HashMap};

    pub enum MatchMode {
        Any,
        All,
    }

    pub struct Options {
        pub mode: MatchMode,
        pub headers: HashMap<String, String>,
    }

    impl ApplyOptionsTo<Attach> for Options {
        fn apply_options_to(&self, target: &mut Attach) {
            if let Some(source) = target.source.as_mut() {
                source.filter = Some({
                    let mut map = BTreeMap::new();
                    map.insert(
                        Symbol::from_string("selector"),
                        Value::Described(
                            Box::new(Value::Symbol(
                                Symbol::from_string("apache.org:legacy-amqp-headers-binding:map")
                                    .to_slice()
                                    .to_vec(),
                            )),
                            Box::new(Value::Map({
                                let mut key_values = vec![(
                                    Value::String("x-match".into()),
                                    Value::String(
                                        match self.mode {
                                            MatchMode::Any => "any",
                                            MatchMode::All => "all",
                                        }
                                        .into(),
                                    ),
                                )];
                                self.headers.iter().for_each(|(key, value)| {
                                    key_values.push((
                                        Value::String(key.clone()),
                                        Value::String(value.clone()),
                                    ));
                                });
                                key_values
                            })),
                        ),
                    );
                    map
                });
            }
        }
    }
}
