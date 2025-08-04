mod config;
mod sink;

#[cfg(all(test, feature = "journald-integration-tests"))]
mod integration_tests;

pub use config::JournaldSinkConfig;