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

pub enum ReceiverFilter {
    ApacheLegacyExchangeHeaderFilter(apache_legacy_exchange_headers_filter::Options),
}

impl ApplyOptionsTo<Attach> for ReceiverFilter {
    fn apply_options_to(&self, target: &mut Attach) {
        match self {
            ReceiverFilter::ApacheLegacyExchangeHeaderFilter(filter) => {
                filter.apply_options_to(target)
            }
        }
    }
}

impl ReceiverFilter {
    pub fn apache_legacy_exchange_headers_match_any<A: Into<String>, B: Into<String>>(
        kv_pairs: impl Iterator<Item = (A, B)>,
    ) -> Self {
        Self::apache_legacy_exchange_headers(
            apache_legacy_exchange_headers_filter::MatchMode::Any,
            kv_pairs,
        )
    }

    pub fn apache_legacy_exchange_headers_match_all<A: Into<String>, B: Into<String>>(
        kv_pairs: impl Iterator<Item = (A, B)>,
    ) -> Self {
        Self::apache_legacy_exchange_headers(
            apache_legacy_exchange_headers_filter::MatchMode::All,
            kv_pairs,
        )
    }

    fn apache_legacy_exchange_headers<A: Into<String>, B: Into<String>>(
        mode: apache_legacy_exchange_headers_filter::MatchMode,
        kv_pairs: impl Iterator<Item = (A, B)>,
    ) -> Self {
        Self::ApacheLegacyExchangeHeaderFilter(apache_legacy_exchange_headers_filter::Options {
            mode,
            headers: kv_pairs
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        })
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
