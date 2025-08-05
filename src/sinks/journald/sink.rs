
use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt};
use snafu::{Snafu};
use tracing::{error, warn};
use vector_lib::internal_event::{
    ByteSize, BytesSent, CountByteSize, EventsSent, InternalEventHandle as _, Output,
    Protocol,
};
use vector_lib::EstimatedJsonEncodedSizeOf;

use crate::sinks::journald::journald_writer::JournaldWriter;
use crate::{
    event::{Event, EventStatus, Finalizable, LogEvent},
    sinks::{journald::config::JournaldSinkConfig, util::StreamSink},
};

#[derive(Debug, Snafu)]
pub enum JournaldSinkError {
    #[snafu(display("Failed to send log to journald: {}", source))]
    Send { source: std::io::Error },
    #[snafu(display("Failed to initialize journald writer: {}", source))]
    Init { source: std::io::Error },
}

pub struct JournaldSink {
    config: JournaldSinkConfig,
    writer: JournaldWriter,
}

impl JournaldSink {
    pub fn new(config: JournaldSinkConfig) -> crate::Result<Self> {
        let path = config
            .journald_path
            .as_deref()
            .unwrap_or("/run/systemd/journal/socket");
        let writer =
            JournaldWriter::new(path).map_err(|e| JournaldSinkError::Init { source: e })?;
        Ok(Self { config, writer })
    }

    fn send_log_to_journal(&mut self, log: &LogEvent) -> Result<(), JournaldSinkError> {

        // Extract the message field
        if let Some(message) = log.get_message() {
            self.writer.add_str("MESSAGE", message.to_string_lossy().as_ref());
        }

        // Extract priority/level if available
        // if let Some(level) = log.get_by_meaning("level") {
        //     let priority = match level.to_string_lossy().as_ref() {
        //         "trace" => "7",         // LOG_DEBUG
        //         "debug" => "6",         // LOG_INFO
        //         "info" => "5",          // LOG_WARNING
        //         "warn" => "4",          // LOG_ERR
        //         "error" => "3",         // LOG_CRIT
        //         "fatal" => "2",
        //         _ => "6",               // Default to LOG_INFO
        //     };
        //     self.writer.add_str("PRIORITY", priority);
        // }

        // Add any additional configured fields
        for (key, value) in &self.config.fields {
            self.writer.add_str(key.as_str(), value.as_str());
        }

        // Add other relevant fields from the log event
        if let Some(all_fields) = log.all_event_fields() {
            for (key, value) in all_fields {
                let key_str = key.to_string();
                // Skip fields we've already handled or internal fields
                if !matches!(key_str.as_str(), "message" | "level" | "timestamp")
                    && !key_str.starts_with('.')
                {
                    self.writer.add_str(&key_str, value.to_string().as_ref());
                }
            }
        }

        self.writer.flush().map_err(|err|JournaldSinkError::Send {source: err})?;

        Ok(())
    }
}

#[async_trait]
impl StreamSink<Event> for JournaldSink {
    async fn run(mut self: Box<Self>, mut input: BoxStream<'_, Event>) -> Result<(), ()> {
        let events_sent = register!(EventsSent::from(Output(None)));
        let bytes_sent = register!(BytesSent::from(Protocol("journald".into())));

        while let Some(mut event) = input.next().await {
            let event_byte_size = event.estimated_json_encoded_size_of();
            let finalizers = event.take_finalizers();

            match event {
                Event::Log(ref log) => match self.send_log_to_journal(log) {
                    Ok(()) => {
                        finalizers.update_status(EventStatus::Delivered);
                        events_sent.emit(CountByteSize(1, event_byte_size));
                        bytes_sent.emit(ByteSize(event_byte_size.get()));
                    }
                    Err(error) => {
                        error!(message = "Failed to send event to journald.", %error);
                        finalizers.update_status(EventStatus::Errored);
                    }
                },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::LogEvent;

    #[test]
    fn test_journald_sink_creation() {
        let config = JournaldSinkConfig::default();
        let sink = JournaldSink::new(config);
        assert!(sink.is_ok());
    }

    #[test]
    fn test_journal_field_mapping() {
        let config = JournaldSinkConfig::default();
        let mut sink = JournaldSink::new(config).unwrap();

        let mut log = LogEvent::from("test message");
        log.insert("level", "info");
        log.insert("host", "test-host");

        // Just test that the function doesn't panic
        // We can't actually test journal sending in unit tests without systemd
        // This would be better tested in integration tests
        let result = sink.send_log_to_journal(&log);
        // We expect this to fail in test environment without systemd
        assert!(result.is_err());
    }
}
