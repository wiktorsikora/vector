use std::collections::HashMap;

use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt};
use snafu::{ResultExt, Snafu};
use systemd::journal;
use tracing::{error, warn};
use vector_lib::internal_event::{
    register, ByteSize, BytesSent, CountByteSize, EventsSent, InternalEventHandle as _, Output, Protocol,
};
use vector_lib::EstimatedJsonEncodedSizeOf;

use crate::{
    event::{Event, EventStatus, Finalizable, LogEvent},
    sinks::{journald::config::JournaldSinkConfig, util::StreamSink},
};

#[derive(Debug, Snafu)]
pub enum JournaldSinkError {
    #[snafu(display("Failed to send log to journald: {}", source))]
    Send { source: std::io::Error },
}

pub struct JournaldSink {
    config: JournaldSinkConfig,
}

impl JournaldSink {
    pub fn new(config: JournaldSinkConfig) -> crate::Result<Self> {
        Ok(Self { config })
    }

    fn send_log_to_journal(&self, log: &LogEvent) -> Result<(), JournaldSinkError> {
        let mut vars = HashMap::new();

        // Extract the message field
        if let Some(message) = log.get("message") {
            vars.insert("MESSAGE", message.to_string_lossy());
        }

        // Extract priority if available
        if let Some(level) = log.get("level") {
            let priority = match level.to_string_lossy().as_ref() {
                "trace" | "debug" => "7", // LOG_DEBUG
                "info" => "6",             // LOG_INFO
                "warn" => "4",             // LOG_WARNING
                "error" => "3",            // LOG_ERR
                "fatal" => "2",            // LOG_CRIT
                _ => "6",                  // Default to LOG_INFO
            };
            vars.insert("PRIORITY", priority.to_string());
        }

        // Add the identifier
        vars.insert("SYSLOG_IDENTIFIER", self.config.identifier.clone());

        // Add any additional configured fields
        for (key, value) in &self.config.fields {
            vars.insert(key.as_str(), value.clone());
        }

        // Add other relevant fields from the log event
        for (key, value) in log.all_event_fields().unwrap_or_default() {
            let key_str = key.to_string();
            // Skip fields we've already handled or internal fields
            if !matches!(key_str.as_str(), "message" | "level" | "timestamp")
                && !key_str.starts_with('.')
            {
                let field_name = key_str.to_uppercase();
                vars.insert(field_name.as_str(), value.to_string_lossy());
            }
        }

        // Convert HashMap<&str, String> to HashMap<String, String> for systemd crate
        let journal_vars: HashMap<String, String> = vars
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();

        journal::send(&journal_vars).context(SendSnafu)?;
        Ok(())
    }
}

#[async_trait]
impl StreamSink<Event> for JournaldSink {
    async fn run(self: Box<Self>, mut input: BoxStream<'_, Event>) -> Result<(), ()> {
        let events_sent = register!(EventsSent::from(Output(None)));
        let bytes_sent = register!(BytesSent::from(Protocol("journald".into())));

        while let Some(mut event) = input.next().await {
            let event_byte_size = event.estimated_json_encoded_size_of();
            let finalizers = event.take_finalizers();

            match event {
                Event::Log(ref log) => {
                    match self.send_log_to_journal(log) {
                        Ok(()) => {
                            finalizers.update_status(EventStatus::Delivered);
                            events_sent.emit(CountByteSize(1, event_byte_size));
                            bytes_sent.emit(ByteSize(event_byte_size.get()));
                        }
                        Err(error) => {
                            error!(message = "Failed to send event to journald.", %error);
                            finalizers.update_status(EventStatus::Errored);
                        }
                    }
                }
                _ => {
                    // For non-log events, we don't process them
                    finalizers.update_status(EventStatus::Errored);
                    warn!("Journald sink received non-log event, skipping.");
                }
            }
        }

        Ok(())
    }
}