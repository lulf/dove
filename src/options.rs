use crate::container::Value;
use crate::framing::{Attach, LinkRole};
use crate::symbol::Symbol;
use std::collections::BTreeMap;

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

    pub fn dynamic(&self) -> Option<bool> {
        match self {
            LinkOptions::Sender(s) => s.as_ref().and_then(|s| s.dynamic.as_ref()),
            LinkOptions::Receiver(r) => r.as_ref().and_then(|r| r.dynamic.as_ref()),
        }
        .map(|d| DynamicFlag::NotDynamic != *d)
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

/// http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-node-properties
#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum DynamicLifetimePolicy {
    DeleteOnClose,
    DeleteOnNoLinks,
    DeleteOnNoMessages,
    DeleteOnNoLinksOrMessages,
}

impl DynamicLifetimePolicy {
    pub(crate) fn symbol(&self) -> Symbol {
        Symbol::from_static_str(match self {
            DynamicLifetimePolicy::DeleteOnClose => "amqp:delete-on-close:list",
            DynamicLifetimePolicy::DeleteOnNoLinks => "amqp:delete-on-no-links:list",
            DynamicLifetimePolicy::DeleteOnNoMessages => "amqp:delete-on-no-messages:list",
            DynamicLifetimePolicy::DeleteOnNoLinksOrMessages => {
                "amqp:delete-on-no-links-or-messages:list"
            }
        })
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum DynamicFlag {
    NotDynamic,
    Dynamic {
        lifetime_policy: DynamicLifetimePolicy,
        // TODO missing supported-dist-modes
    },
}

impl From<DynamicLifetimePolicy> for DynamicFlag {
    fn from(lifetime_policy: DynamicLifetimePolicy) -> Self {
        Self::Dynamic { lifetime_policy }
    }
}

impl ApplyOptionsTo<(&mut Option<bool>, &mut Option<BTreeMap<Symbol, Value>>)> for DynamicFlag {
    fn apply_options_to(
        &self,
        (dynamic, dynamic_node_properties): &mut (
            &mut Option<bool>,
            &mut Option<BTreeMap<Symbol, Value>>,
        ),
    ) {
        match self {
            DynamicFlag::NotDynamic => **dynamic = Some(false),
            DynamicFlag::Dynamic { lifetime_policy } => {
                **dynamic = Some(true);
                dynamic_node_properties
                    .get_or_insert_with(Default::default)
                    .insert(
                        Symbol::from_static_str("lifetime-policy"),
                        Value::Described(
                            Box::new(lifetime_policy.symbol().into()),
                            Box::new(Value::List(Vec::new())),
                        ),
                    );
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct SenderOptions {
    /// Whether to create the exchange point dynamically, if it does not yet exist
    pub dynamic: Option<DynamicFlag>,
}

impl SenderOptions {
    pub fn with_dynamic_flag(mut self, dynamic: impl Into<DynamicFlag>) -> Self {
        self.dynamic = Some(dynamic.into());
        self
    }
}

impl ApplyOptionsTo<Attach> for SenderOptions {
    fn apply_options_to(&self, attach: &mut Attach) {
        if let (Some(dynamic_flag), Some(target)) = (&self.dynamic, &mut attach.target) {
            dynamic_flag
                .apply_options_to(&mut (&mut target.dynamic, &mut target.dynamic_node_properties))
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ReceiverOptions {
    pub filter: Option<ReceiverFilter>,
    /// Whether to create the exchange point dynamically, if it does not yet exist
    pub dynamic: Option<DynamicFlag>,
}

impl ReceiverOptions {
    pub fn with_filter(mut self, filter: ReceiverFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_dynamic_flag(mut self, dynamic: impl Into<DynamicFlag>) -> Self {
        self.dynamic = Some(dynamic.into());
        self
    }
}

#[allow(clippy::needless_update)]
impl From<ReceiverFilter> for ReceiverOptions {
    fn from(filter: ReceiverFilter) -> Self {
        Self::default().with_filter(filter)
    }
}

impl ApplyOptionsTo<Attach> for ReceiverOptions {
    fn apply_options_to(&self, target: &mut Attach) {
        self.filter.apply_options_to(target);

        if let (Some(dynamic_flag), Some(source)) = (&self.dynamic, &mut target.source) {
            dynamic_flag
                .apply_options_to(&mut (&mut source.dynamic, &mut source.dynamic_node_properties));
        }
    }
}

/// Sets the filter to be applied by the broker before sending the message
/// through the link. Be aware, that some filters do nothing unless the correct
/// exchange type is set.
#[derive(Debug, Clone)]
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

    /// Complex filter based on values set in `message.application_properties.*`.
    /// See 'apache.org:legacy-amqp-headers-binding:map'
    ///
    /// # WARN
    /// Requires exchange type 'headers'
    ApacheSelector(apache_selector::Selector),
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
            ReceiverFilter::ApacheSelector(filter) => filter.apply_options_to(target),
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
    pub fn apache_legacy_exchange_headers_binding_match_any<A: Into<Value>, B: Into<Value>>(
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
    pub fn apache_legacy_exchange_headers_binding_match_all<A: Into<Value>, B: Into<Value>>(
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
    fn apache_legacy_exchange_headers_binding<A: Into<Value>, B: Into<Value>>(
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

    /// Filter based on some SQL-like syntax.
    /// See 'apache.org:selector-filter:string', 'JMS Selectors' and 'javax.jmx.Message'.
    /// https://docs.oracle.com/javaee/1.4/api/javax/jms/Message.html
    ///
    /// # WARN
    /// Requires exchange type 'headers'
    pub fn apache_selector(query: impl Into<String>) -> Self {
        Self::ApacheSelector(apache_selector::Selector {
            query: query.into(),
        })
    }
}

pub mod apache_legacy_exchange_direct_binding {
    use super::*;
    use crate::symbol::Symbol;
    use crate::types::Value;
    use std::collections::BTreeMap;

    #[derive(Debug, Clone)]
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
                        Symbol::from_static_str(filter_symbol),
                        Value::Described(
                            Box::new(Value::from(Symbol::from_static_str(filter_symbol))),
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
    use std::collections::BTreeMap;

    #[derive(Debug, Clone)]
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
                        Symbol::from_static_str(filter_symbol),
                        Value::Described(
                            Box::new(Value::from(Symbol::from_static_str(filter_symbol))),
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
    use std::collections::BTreeMap;

    #[derive(Debug, Clone, Copy)]
    pub enum MatchMode {
        Any,
        All,
    }

    #[derive(Debug, Clone)]
    pub struct Options {
        pub mode: MatchMode,
        pub headers: Vec<(Value, Value)>,
    }

    impl ApplyOptionsTo<Attach> for Options {
        fn apply_options_to(&self, target: &mut Attach) {
            if let Some(source) = target.source.as_mut() {
                source.filter = Some({
                    let mut map = BTreeMap::new();
                    map.insert(
                        Symbol::from_static_str("selector"),
                        Value::Described(
                            Box::new(Value::from(Symbol::from_static_str(
                                "apache.org:legacy-amqp-headers-binding:map",
                            ))),
                            Box::new(Value::Map({
                                let mut key_values = vec![(
                                    Value::Str("x-match"),
                                    Value::Str(match self.mode {
                                        MatchMode::Any => "any",
                                        MatchMode::All => "all",
                                    }),
                                )];

                                key_values.extend_from_slice(&self.headers);
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

pub mod apache_selector {
    use super::*;
    use crate::symbol::Symbol;
    use crate::types::Value;
    use std::collections::BTreeMap;

    #[derive(Debug, Clone)]
    pub struct Selector {
        pub query: String,
    }

    impl ApplyOptionsTo<Attach> for Selector {
        fn apply_options_to(&self, target: &mut Attach) {
            if let Some(source) = target.source.as_mut() {
                source.filter = Some({
                    let mut map = BTreeMap::new();
                    map.insert(
                        Symbol::from_static_str("selector"),
                        Value::Described(
                            Box::new(Value::from(Symbol::from_static_str(
                                "apache.org:selector-filter:string",
                            ))),
                            Box::new(Value::String(self.query.clone())),
                        ),
                    );
                    map
                });
            }
        }
    }
}
