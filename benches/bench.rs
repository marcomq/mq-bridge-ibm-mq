use criterion::{Criterion, criterion_group, criterion_main};
use mq_bridge::canonical_message::CanonicalMessage;
use mq_bridge::test_utils::mq_config;
use mq_bridge::traits::CustomEndpointFactory;
use mq_bridge_ibm_mq::IbmMqFactory;
use tokio::runtime::Runtime;

fn benchmark_publish(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let factory = IbmMqFactory;
    let config = mq_config("DEV.QUEUE.1");

    let publisher = rt.block_on(async {
        factory
            .create_publisher("bench_pub", &config)
            .await
            .expect("Failed to create publisher")
    });

    let message = CanonicalMessage::new(b"Benchmark payload".to_vec(), None);

    c.bench_function("ibm_mq_publish", |b| {
        b.to_async(&rt).iter(|| async {
            publisher.send_batch(vec![message.clone()]).await.unwrap();
        })
    });
}

criterion_group!(benches, benchmark_publish);
criterion_main!(benches);
