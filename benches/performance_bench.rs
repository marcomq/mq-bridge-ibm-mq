use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mq_bridge::bench_backend;
use mq_bridge::test_utils::{PERF_TEST_CONCURRENCY, PerformanceResult, print_benchmark_results};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

const PERF_TEST_MESSAGE_COUNT: usize = 1000;

static BENCH_RESULTS: Lazy<Mutex<HashMap<String, PerformanceResult>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub mod ibm_mq_helper {
    use mq_bridge::traits::{MessageConsumer, MessagePublisher};
    use mq_bridge_ibm_mq::{IbmMqConfig, IbmMqEndpoint};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    pub fn get_config() -> IbmMqConfig {
        IbmMqConfig {
            user: Some("app".to_string()),
            password: Some("admin".to_string()),
            queue_manager: "QM1".to_string(),
            connection_name: "localhost(1414)".to_string(),
            channel: "DEV.APP.SVRCONN".to_string(),
            ..Default::default()
        }
    }

    pub async fn create_publisher() -> Arc<dyn MessagePublisher> {
        let endpoint_config = IbmMqEndpoint {
            queue: Some("DEV.QUEUE.1".to_string()),
            topic: None,
            config: get_config(),
        };

        let publisher = mq_bridge_ibm_mq::create_ibm_mq_publisher("bench_pub", &endpoint_config)
            .await
            .expect("Failed to create publisher");
        Arc::new(publisher)
    }

    pub async fn create_consumer() -> Arc<Mutex<dyn MessageConsumer>> {
        let endpoint_config = IbmMqEndpoint {
            queue: Some("DEV.QUEUE.1".to_string()),
            topic: None,
            config: get_config(),
        };

        let consumer = mq_bridge_ibm_mq::create_ibm_mq_consumer("bench_sub", &endpoint_config)
            .await
            .expect("Failed to create consumer");
        Arc::new(Mutex::new(consumer))
    }
}

fn performance_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("performance");
    // Since these are integration tests involving network/disk, we reduce sample size
    // and increase measurement time to accommodate their duration.
    group.sample_size(10);
    group.throughput(Throughput::Elements(PERF_TEST_MESSAGE_COUNT as u64));
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(1));

    bench_backend!(
        "",
        "ibm-mq",
        "tests/docker-compose.yml",
        ibm_mq_helper,
        group,
        &rt,
        &BENCH_RESULTS,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY
    );

    // Print consolidated results
    let results = BENCH_RESULTS.blocking_lock();
    print_benchmark_results(&results, PERF_TEST_MESSAGE_COUNT);
    group.finish();
}

criterion_group!(benches, performance_benchmarks);
criterion_main!(benches);
