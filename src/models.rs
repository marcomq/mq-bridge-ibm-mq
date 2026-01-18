use mq_bridge::models::TlsConfig;
use serde::{Deserialize, Serialize};

/// Configuration for an IBM MQ Endpoint.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct IbmMqEndpoint {
    /// Target Queue name for point-to-point messaging. Optional if `topic` is set; defaults to route name if omitted.
    pub queue: Option<String>,
    /// Target Topic string for Publish/Subscribe. If set, enables subscriber mode. Optional if `queue` is set.
    pub topic: Option<String>,
    /// Connection details for the Queue Manager.
    #[serde(flatten)]
    pub config: IbmMqConfig,
}

/// Connection settings for the IBM MQ Queue Manager.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct IbmMqConfig {
    /// Required. Connection string in `host(port)` format. Supports comma-separated list for failover (e.g., `host1(1414),host2(1414)`).
    pub connection_name: String,
    /// Required. Name of the Queue Manager to connect to (e.g., `QM1`).
    pub queue_manager: String,
    /// Required. Server Connection (SVRCONN) Channel name defined on the QM.
    pub channel: String,
    /// Username for authentication. Optional; required if the channel enforces authentication.
    pub user: Option<String>,
    /// Password for authentication. Optional; required if the channel enforces authentication.
    pub password: Option<String>,
    /// TLS CipherSpec (e.g., `ANY_TLS12`). Optional; required for encrypted connections.
    pub cipher_spec: Option<String>,
    /// TLS configuration settings (e.g., keystore paths). Optional.
    #[serde(default)]
    pub tls: TlsConfig,
    /// Maximum message size in bytes (default: 4MB). Optional.
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,
    /// Polling timeout in milliseconds (default: 1000ms). Optional.
    #[serde(default = "default_wait_timeout_ms")]
    pub wait_timeout_ms: i32,
}

fn default_max_message_size() -> usize {
    4 * 1024 * 1024 // 4MB default
}

fn default_wait_timeout_ms() -> i32 {
    1000 // 1 second default
}
