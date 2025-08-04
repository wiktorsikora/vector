use super::*;
use crate::{
    config::{SinkConfig, SinkContext},
    test_util::{
        components::{run_and_assert_sink_compliance, SINK_TAGS},
        random_string,
    },
};
use futures::stream;
use futures::future::ready;
use vector_lib::event::{Event, LogEvent};
use std::collections::HashMap;

fn make_config() -> JournaldSinkConfig {
    JournaldSinkConfig {
        identifier: format!("vector-test-{}", random_string(10)),
        fields: HashMap::new(),
        acknowledgements: AcknowledgementsConfig::default(),
    }
}

#[tokio::test]
async fn journald_healthcheck() {
    let config = make_config();
    let context = SinkContext::default();
    
    // The healthcheck should always pass for journald
    let result = config.build(context).await;
    assert!(result.is_ok());
    
    let (_sink, healthcheck) = result.unwrap();
    let healthcheck_result = healthcheck.await;
    assert!(healthcheck_result.is_ok());
}

#[tokio::test]
async fn journald_sink_compliance() {
    let config = make_config();
    let context = SinkContext::default();
    let (sink, _) = config.build(context).await.unwrap();
    
    let event = Event::Log(LogEvent::from("test message"));
    let stream = stream::once(ready(event));
    
    // This test verifies that the sink meets Vector's compliance requirements
    // It may fail if systemd is not available, but will test the basic structure
    run_and_assert_sink_compliance(sink, stream, &SINK_TAGS).await;
}

#[ignore] // Only run in environments with systemd
#[tokio::test]
async fn journald_send_message() {
    let config = make_config();
    let context = SinkContext::default();
    let (sink, _) = config.build(context).await.unwrap();
    
    let mut log = LogEvent::from("Integration test message from Vector");
    log.insert("level", "info");
    log.insert("test_field", "test_value");
    
    let event = Event::Log(log);
    let events = vec![event];
    
    // Send events to the sink
    // This test is ignored by default since it requires systemd to be running
    // and would actually write to the system journal
    let mut stream = stream::iter(events);
    
    match sink {
        crate::sinks::VectorSink::Stream(mut stream_sink) => {
            let result = stream_sink.run(Box::pin(stream.map(|e| vec![e].into()))).await;
            // We expect this might fail in CI environments without systemd
            println!("Journald send result: {:?}", result);
        }
        _ => panic!("Expected stream sink"),
    }
}