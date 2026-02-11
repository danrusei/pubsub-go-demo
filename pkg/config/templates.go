package config

// Example configuration templates exposed via `loadtest init`.

const SimpleTemplate = `# Test configuration
test_name: "simple_throughput_test"
description: "Basic throughput test with one topic and shared subscription"

danube:
  service_url: "127.0.0.1:6650"

execution:
  duration: "2m"
  warmup_duration: "5s"
  cooldown_duration: "3s"

topics:
  - name: "/default/load_simple_test"
    partitions: 0
    dispatch_strategy: "non_reliable"

producers:
  - name: "test_producers"
    topic: "/default/load_simple_test"
    count: 3
    rate_per_second: 50
    message_size: 256

consumers:
  - name: "test_consumers"
    topic: "/default/load_simple_test"
    subscription: "sub_shared"
    subscription_type: "shared"
    count: 3

metrics:
  enabled: true
  report_interval: "5s"
  percentiles: [50, 95, 99]
  output_format: "terminal"
  export_path: "./results"
  collect:
    - producer_throughput
    - consumer_throughput
    - end_to_end_latency
    - producer_latency
    - error_rates
`

const StressTemplate = `# Stress configuration across multiple topics
test_name: "multi_topic_stress_test"
description: "Stress test with multiple topics, partitions and subscription types"

danube:
  service_url: "127.0.0.1:6650"

execution:
  duration: "3m"
  warmup_duration: "10s"
  cooldown_duration: "5s"

topics:
  - name: "/default/orders"
    partitions: 3
    dispatch_strategy: "reliable"
    schema:
      type: "json_schema"
      definition: |
        {"type":"object","properties":{"order_id":{"type":"string"},"amount":{"type":"number"},"ts":{"type":"integer"}},"required":["order_id","amount"]}
  - name: "/default/events"
    partitions: 1
    dispatch_strategy: "non_reliable"
  - name: "/default/metrics"
    partitions: 0
    dispatch_strategy: "non_reliable"

producers:
  - name: "order_producers"
    topic: "/default/orders"
    count: 5
    rate_per_second: 100
    message_size: 1024
  - name: "event_producers"
    topic: "/default/events"
    count: 3
    rate_per_second: 50
    message_size: 512
  - name: "metric_producers"
    topic: "/default/metrics"
    count: 2
    rate_per_second: 200
    message_size: 64

consumers:
  - name: "order_processor"
    topic: "/default/orders"
    subscription: "order_processing"
    subscription_type: "shared"
    count: 3
  - name: "order_analytics"
    topic: "/default/orders"
    subscription: "analytics"
    subscription_type: "exclusive"
    count: 1
  - name: "order_backup"
    topic: "/default/orders"
    subscription: "backup"
    subscription_type: "failover"
    count: 2
  - name: "event_handler"
    topic: "/default/events"
    subscription: "event_processing"
    subscription_type: "shared"
    count: 2

metrics:
  enabled: true
  report_interval: "5s"
  percentiles: [50, 95, 99, 99.9]
  output_format: "terminal"
  export_path: "./results"
  collect:
    - producer_throughput
    - consumer_throughput
    - end_to_end_latency
    - producer_latency
    - message_loss
    - error_rates
`

const PatternsTemplate = `# Test configuration
test_name: "patterns_test"
description: "Covers multiple topic patterns: partitions, schemas, and subscription mixes"

danube:
  service_url: "127.0.0.1:6650"

execution:
  duration: "2m"
  warmup_duration: "5s"
  cooldown_duration: "3s"

topics:
  - name: "/default/pattern_1"
    partitions: 0
    dispatch_strategy: "non_reliable"
    schema:
      type: "number"

  - name: "/default/pattern_2"
    partitions: 3
    dispatch_strategy: "non_reliable"
    schema:
      type: "json_schema"
      definition: |
        {"type":"object","properties":{"seq":{"type":"integer"},"msg":{"type":"string"}},"required":["seq","msg"]}

  - name: "/default/pattern_3"
    partitions: 0
    dispatch_strategy: "reliable"

  - name: "/default/pattern_4"
    partitions: 3
    dispatch_strategy: "reliable"

  - name: "/default/pattern_5"
    partitions: 3
    dispatch_strategy: "reliable"

producers:
  - name: "pattern_1_prod"
    topic: "/default/pattern_1"
    count: 1
    rate_per_second: 20
    message_size: 32

  - name: "pattern_2_prod"
    topic: "/default/pattern_2"
    count: 2
    rate_per_second: 15
    message_size: 256

  - name: "pattern_3_prod"
    topic: "/default/pattern_3"
    count: 1
    rate_per_second: 20
    message_size: 128

  - name: "pattern_4_prod"
    topic: "/default/pattern_4"
    count: 1
    rate_per_second: 20
    message_size: 128

  - name: "pattern_5_prod"
    topic: "/default/pattern_5"
    count: 2
    rate_per_second: 15
    message_size: 128

consumers:
  - name: "c_pattern_1_shared"
    topic: "/default/pattern_1"
    subscription: "s_pattern_1_shared_1"
    subscription_type: "shared"
    count: 2
  - name: "c_pattern_1_excl_1"
    topic: "/default/pattern_1"
    subscription: "s_pattern_1_excl_1"
    subscription_type: "exclusive"
    count: 1

  - name: "c_pattern_2_shared"
    topic: "/default/pattern_2"
    subscription: "s_pattern_2_shared_1"
    subscription_type: "shared"
    count: 3

  - name: "c_pattern_3_shared"
    topic: "/default/pattern_3"
    subscription: "s_pattern_3_shared_1"
    subscription_type: "shared"
    count: 2
  - name: "c_pattern_3_excl_1"
    topic: "/default/pattern_3"
    subscription: "s_pattern_3_excl_1"
    subscription_type: "exclusive"
    count: 1

  - name: "c_pattern_4_shared"
    topic: "/default/pattern_4"
    subscription: "s_pattern_4_shared_1"
    subscription_type: "shared"
    count: 3

  - name: "c_pattern_5_shared"
    topic: "/default/pattern_5"
    subscription: "s_pattern_5_shared_1"
    subscription_type: "shared"
    count: 2
  - name: "c_pattern_5_excl_1"
    topic: "/default/pattern_5"
    subscription: "s_pattern_5_excl_1"
    subscription_type: "exclusive"
    count: 1
`
