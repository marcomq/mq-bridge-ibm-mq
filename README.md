# mq-bridge-ibm-mq

IBM MQ endpoint implementation for the `mq-bridge` framework.
This crate allows `mq-bridge` to consume messages from and publish messages to IBM MQ queues and topics.

## Prerequisites

This crate relies on the `mqi` crate, which requires the **IBM MQ Client** libraries (C runtime) to be installed on the system where the application runs.

## Usage

Add this crate to your `Cargo.toml` alongside `mq-bridge`.

In your application initialization, register the factory:

```rust
use mq_bridge_ibm_mq::IbmMqFactory;
use std::sync::Arc;

// ... inside your setup code
mq_bridge::route::register_endpoint_factory("ibm_mq", Arc::new(IbmMqFactory));
```

## Configuration

You can configure the endpoint in your `mq-bridge` configuration (e.g., YAML).

### Example

```yaml
routes:
  my_mq_route:
    input:
      ibm_mq:
        connection_name: "localhost(1414)"
        queue_manager: "QM1"
        channel: "DEV.APP.SVRCONN"
        queue: "INCOMING.QUEUE"
        user: "app_user"
        password: "secret_password"
    output:
      ibm_mq:
        connection_name: "localhost(1414)"
        queue_manager: "QM1"
        channel: "DEV.APP.SVRCONN"
        topic: "NOTIFICATIONS/ALERTS"
```

### Configuration Options

The configuration accepts the following fields:

| Field | Type | Description |
|-------|------|-------------|
| `connection_name` | String | **Required**. The connection string, typically `host(port)`. Supports comma-separated list for failover. |
| `queue_manager` | String | **Required**. The name of the Queue Manager. |
| `channel` | String | **Required**. The server connection channel name. |
| `queue` | String | The name of the queue to consume from or publish to. |
| `topic` | String | The topic string. If provided, the endpoint acts as a subscriber/publisher to this topic. |
| `user` | String | (Optional) Username for authentication. |
| `password` | String | (Optional) Password for authentication. |
| `cipher_spec` | String | (Optional) Cipher specification for TLS connections. |
| `tls` | Object | (Optional) TLS configuration (e.g., `cert_file`). |