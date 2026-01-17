#![allow(dead_code)]

use mq_bridge::models::{ConsumerMode, Endpoint, EndpointType, Route};
use mq_bridge::route::register_endpoint_factory;
/// This test requires a running IBM MQ instance.
/// You can use the provided docker-compose file:
/// `docker-compose -f tests/docker-compose.yml up -d`
use mq_bridge::test_utils::{
    add_performance_result, generate_test_messages, run_chaos_pipeline_test, run_direct_perf_test,
    run_test_with_docker, run_test_with_docker_controller, setup_logging,
};
use mq_bridge::traits::MessagePublisher;
use mq_bridge_ibm_mq::{IbmMqConfig, IbmMqConsumer, IbmMqEndpoint, IbmMqFactory, IbmMqPublisher};
use std::sync::Arc;
use std::time::Instant;

fn get_config() -> IbmMqConfig {
    IbmMqConfig {
        user: Some("app".to_string()),
        password: Some("admin".to_string()),
        queue_manager: "QM1".to_string(),
        connection_name: "localhost(1414)".to_string(),
        channel: "DEV.APP.SVRCONN".to_string(),
        ..Default::default()
    }
}

#[tokio::test]
pub async fn test_ibm_mq_performance_pipeline() {
    setup_logging();
    register_endpoint_factory("ibm_mq", Arc::new(IbmMqFactory));
    run_test_with_docker("tests/docker-compose.yml", || async {
        let queue_name = "DEV.QUEUE.1";
        let config = get_config();

        // Seed the queue
        let publisher = IbmMqPublisher::new(config.clone(), queue_name.to_string())
            .await
            .expect("Failed to create publisher");

        let num_messages = 1000;
        let messages = generate_test_messages(num_messages);
        publisher
            .send_batch(messages)
            .await
            .expect("Failed to seed queue");

        // Setup Pipeline: IBM MQ -> Memory
        let input_ep = Endpoint {
            endpoint_type: EndpointType::Custom {
                name: "ibm_mq".to_string(),
                config: serde_json::to_value(IbmMqEndpoint {
                    config: config.clone(),
                    topic: None,
                    queue: Some(queue_name.to_string()),
                })
                .unwrap(),
            },
            mode: ConsumerMode::Consume,
            middlewares: vec![],
            handler: None,
        };
        let output_ep = Endpoint::new_memory("mem_out", num_messages);

        let mut route = Route::new(input_ep, output_ep);
        route.concurrency = 4;
        route.batch_size = 128;
        let out_channel = route.output.channel().unwrap();

        // Run route in background
        let handle = tokio::spawn(async move {
            let _ = route.run_until_err("ibm_mq_pipe", None, None).await;
        });

        // Wait for messages
        let start = Instant::now();
        loop {
            if out_channel.len() >= num_messages {
                break;
            }
            if start.elapsed().as_secs() > 90 {
                panic!("Timeout waiting for messages in pipeline");
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        handle.abort();
        println!("IBM MQ Pipeline test passed!");
    })
    .await;
}

#[tokio::test]
pub async fn test_ibm_mq_chaos() {
    setup_logging();
    register_endpoint_factory("ibmmq", Arc::new(IbmMqFactory));
    run_test_with_docker_controller("tests/docker-compose.yml", |controller| async move {
        let config_yaml = r#"
routes:
  memory_to_ibm_mq:
    input:
      memory:
        topic: "chaos_in"
    output:
      ibmmq:
        queue_manager: "QM1"
        connection_name: "localhost(1414)"
        channel: "DEV.APP.SVRCONN"
        queue: "DEV.QUEUE.1"
        user: "app"
        password: "admin"
  ibm_mq_to_memory:
    input:
      ibmmq:
        queue_manager: "QM1"
        connection_name: "localhost(1414)"
        channel: "DEV.APP.SVRCONN"
        queue: "DEV.QUEUE.1"
        user: "app"
        password: "admin"
    output:
      memory:
        topic: "chaos_out"
"#;
        run_chaos_pipeline_test("ibm_mq", config_yaml, controller, "mq").await;
    })
    .await;
}

#[tokio::test]
pub async fn test_ibm_mq_performance_direct() {
    setup_logging();
    run_test_with_docker("tests/docker-compose.yml", || async {
        let queue = "DEV.QUEUE.1";
        let config = get_config();

        let result = run_direct_perf_test(
            "IBM-MQ",
            || async {
                Arc::new(
                    IbmMqPublisher::new(config.clone(), queue.to_string())
                        .await
                        .unwrap(),
                )
            },
            || async {
                Arc::new(tokio::sync::Mutex::new(
                    IbmMqConsumer::new(config.clone(), queue.to_string())
                        .await
                        .unwrap(),
                ))
            },
        )
        .await;
        add_performance_result(result);
    })
    .await;
}
