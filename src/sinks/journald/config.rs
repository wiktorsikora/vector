use std::collections::HashMap;

use futures::{future, FutureExt};
use vector_lib::configurable::configurable_component;

use crate::{
    config::{AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext},
    sinks::{journald::sink::JournaldSink, Healthcheck, VectorSink},
};

/// Configuration for the `journald` sink.
#[configurable_component(sink(
    "journald",
    "Send observability events to the systemd journal for local logging."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct JournaldSinkConfig {
    /// Additional fields to include in journal entries.
    ///
    /// Allows adding custom fields beyond the standard ones like MESSAGE and PRIORITY.
    /// Field names are automatically converted to uppercase as required by systemd journal.
    #[configurable(metadata(docs::examples = "examples_fields()"))]
    #[serde(default)]
    pub fields: HashMap<String, String>,

    /// Path to the journald socket.
    /// If not specified, the default systemd journal socket will be used.
    #[configurable(metadata(docs::examples = "Some(\"/run/systemd/journal/socket\".to_string())"))]
    pub journald_path: Option<String>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

impl Default for JournaldSinkConfig {
    fn default() -> Self {
        Self {
            fields: HashMap::new(),
            journald_path: None,
            acknowledgements: AcknowledgementsConfig::default(),
        }
    }
}

fn examples_fields() -> HashMap<String, String> {
    let mut fields = HashMap::new();
    fields.insert("CUSTOM_FIELD".to_string(), "custom_value".to_string());
    fields
}

impl GenerateConfig for JournaldSinkConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self::default()).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "journald")]
impl SinkConfig for JournaldSinkConfig {
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let sink = JournaldSink::new(self.clone())?;
        let healthcheck = future::ok(()).boxed();

        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<JournaldSinkConfig>();
    }

    #[test]
    fn test_config_default() {
        let config = JournaldSinkConfig::default();
        assert!(config.fields.is_empty());
        assert_eq!(config.journald_path, Some("/run/systemd/journal/socket".to_string()));
    }
}
