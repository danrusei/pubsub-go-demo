# LoadTest_Danube

Danube Messaging load testing tool written in Go. It can spin up multiple producers and consumers across topics, subscription types, partitions, and dispatch strategies, then print a concise console summary with throughput, errors, and end-to-end latency percentiles.

## Prerequisites

- Go 1.24+
- A running Danube broker (single node or cluster)
  - Docs: <https://danube-docs.dev-state.com/>
- macOS/Linux shell

## Install & Build

```bash
go mod tidy
go build -o bin/loadtest ./cmd/loadtest
```

Check the CLI:

```bash
./bin/loadtest --help
./bin/loadtest init --template simple  # writes configs/simple_test.yaml
./bin/loadtest init --template stress  # writes configs/stress_test.yaml
./bin/loadtest init --template patterns  # writes configs/patterns_test.yaml
```

## Quick Start

1) Ensure Danube broker is running and reachable (default used here is 127.0.0.1:6650).

2) Validate a config:

```bash
./bin/loadtest validate --config configs/simple_test.yaml
```

3) Run the load test:

```bash
./bin/loadtest run --config configs/simple_test.yaml
```

You’ll see periodic “Stats:” lines and a final summary including sent/received/errors, tx/rx throughput, and latency percentiles.

## Configuration Overview (YAML)

Top-level keys:

- `test_name`, `description`
- `danube.service_url`: e.g. "127.0.0.1:6650"
- `execution.duration`: test run duration (e.g. "2m")
- `topics[]`: topic definitions (schema, partitions, dispatch)
- `producers[]`: producer groups (topic, count, rate)
- `consumers[]`: consumer groups (topic, subscription, type, count)
- `metrics`: console reporting options (interval)

## Example

Example to test the main Danube usage patterns:

```bash
./bin/loadtest run --config configs/stress_test.yaml

Loaded test 'multi_topic_stress_test' targeting 127.0.0.1:6650
Topics: 3, Producers: 3 groups, Consumers: 4 groups
2026/02/11 17:41:04 runner.go:52: Load test started for 3m0s...
2026/02/11 17:41:09 runner.go:64: Stats: elapsed=6s sent=5440 recv=8239 err=1 tx_mps=893.8 rx_mps=1353.6 lat(ms): p50=1.0 p95=43.0 p99=94.0 max=129.0 n=8239
2026/02/11 17:41:14 runner.go:64: Stats: elapsed=11s sent=9689 recv=16486 err=1 tx_mps=873.9 rx_mps=1487.0 lat(ms): p50=1.0 p95=3.0 p99=81.0 max=129.0 n=16486
2026/02/11 17:41:19 runner.go:64: Stats: elapsed=16s sent=13939 recv=24736 err=1 tx_mps=866.5 rx_mps=1537.7 lat(ms): p50=1.0 p95=3.0 p99=70.0 max=129.0 n=24737
2026/02/11 17:41:24 runner.go:64: Stats: elapsed=21s sent=18190 recv=32989 err=1 tx_mps=862.6 rx_mps=1564.5 lat(ms): p50=1.0 p95=2.0 p99=55.0 max=129.0 n=32989
2026/02/11 17:41:29 runner.go:64: Stats: elapsed=26s sent=22440 recv=41239 err=1 tx_mps=860.2 rx_mps=1580.8 lat(ms): p50=1.0 p95=2.0 p99=43.0 max=129.0 n=41239
2026/02/11 17:41:34 runner.go:64: Stats: elapsed=31s sent=26689 recv=49489 err=1 tx_mps=858.5 rx_mps=1592.0 lat(ms): p50=1.0 p95=2.0 p99=31.0 max=129.0 n=49489
2026/02/11 17:41:39 runner.go:64: Stats: elapsed=36s sent=30940 recv=57739 err=1 tx_mps=857.4 rx_mps=1600.0 lat(ms): p50=1.0 p95=2.0 p99=21.0 max=129.0 n=57739
2026/02/11 17:41:44 runner.go:64: Stats: elapsed=41s sent=35190 recv=65989 err=1 tx_mps=856.5 rx_mps=1606.1 lat(ms): p50=1.0 p95=2.0 p99=14.0 max=129.0 n=65989
2026/02/11 17:41:49 runner.go:64: Stats: elapsed=46s sent=39438 recv=74235 err=1 tx_mps=855.7 rx_mps=1610.8 lat(ms): p50=1.0 p95=2.0 p99=12.0 max=129.0 n=74235
2026/02/11 17:41:54 runner.go:64: Stats: elapsed=51s sent=43690 recv=82489 err=1 tx_mps=855.2 rx_mps=1614.7 lat(ms): p50=1.0 p95=2.0 p99=8.0 max=129.0 n=82489
2026/02/11 17:41:59 runner.go:64: Stats: elapsed=56s sent=47939 recv=90739 err=1 tx_mps=854.7 rx_mps=1617.8 lat(ms): p50=1.0 p95=2.0 p99=6.0 max=129.0 n=90739
2026/02/11 17:42:04 runner.go:64: Stats: elapsed=61s sent=52188 recv=98667 err=1 tx_mps=854.3 rx_mps=1615.2 lat(ms): p50=1.0 p95=2.0 p99=47.0 max=351.0 n=98667
2026/02/11 17:42:09 runner.go:64: Stats: elapsed=66s sent=56439 recv=107237 err=1 tx_mps=854.0 rx_mps=1622.7 lat(ms): p50=1.0 p95=3.0 p99=170.0 max=358.0 n=107237
2026/02/11 17:42:14 runner.go:64: Stats: elapsed=71s sent=60689 recv=115486 err=1 tx_mps=853.7 rx_mps=1624.6 lat(ms): p50=1.0 p95=4.0 p99=158.0 max=358.0 n=115486
2026/02/11 17:42:19 runner.go:64: Stats: elapsed=76s sent=64939 recv=123739 err=1 tx_mps=853.5 rx_mps=1626.3 lat(ms): p50=1.0 p95=5.0 p99=139.0 max=358.0 n=123739
2026/02/11 17:42:24 runner.go:64: Stats: elapsed=81s sent=69190 recv=131989 err=1 tx_mps=853.3 rx_mps=1627.8 lat(ms): p50=1.0 p95=5.0 p99=126.0 max=358.0 n=131989
2026/02/11 17:42:29 runner.go:64: Stats: elapsed=86s sent=73440 recv=140239 err=1 tx_mps=853.1 rx_mps=1629.0 lat(ms): p50=1.0 p95=4.0 p99=118.0 max=358.0 n=140239
2026/02/11 17:42:34 runner.go:64: Stats: elapsed=91s sent=77690 recv=148489 err=1 tx_mps=852.9 rx_mps=1630.2 lat(ms): p50=1.0 p95=4.0 p99=112.0 max=358.0 n=148489
2026/02/11 17:42:39 runner.go:64: Stats: elapsed=96s sent=81940 recv=156651 err=1 tx_mps=852.8 rx_mps=1630.3 lat(ms): p50=1.0 p95=4.0 p99=106.0 max=358.0 n=156651
....
....

026/02/11 17:42:40 report.go:18: 
===== Load Test Summary =====
2026/02/11 17:42:40 report.go:19: Test:        multi_topic_stress_test
2026/02/11 17:42:40 report.go:20: Broker:      127.0.0.1:6650
2026/02/11 17:42:40 report.go:21: Duration:    5m0s (elapsed 97.1s)
2026/02/11 17:42:40 report.go:22: Messages:    sent=82733  received=158277  errors=6
2026/02/11 17:42:40 report.go:23: Throughput:  tx=852.3 msg/s  rx=1630.5 msg/s
2026/02/11 17:42:40 report.go:25: Latency(ms): p50=1.0  p95=4.0  p99=105.0  max=358.0  samples=158277
2026/02/11 17:42:40 report.go:29: Integrity:   loss=0  duplicates=0
2026/02/11 17:42:40 report.go:41: SLA:         keys_in_sla=18  keys_out_sla=0  total_keys=18
2026/02/11 17:42:40 report.go:55: Worst keys (top 5):
2026/02/11 17:42:40 report.go:65: ==============================
2026/02/11 17:42:40 report.go:110: Results exported to results/multi_topic_stress_test_20260211_174240.json
```

## What Metrics Are Reported

- Messages sent, received, errors
- Throughput (tx/rx msgs/sec)
- End-to-end latency (ms): p50, p95, p99, max, sample count (computed from message PublishTime)
- Integrity: estimated message loss and duplicate counts per (topic, subscription, producer)

### Results Export (optional)

- Set `metrics.export_path: "./results"` in your config to also write a JSON file with the final snapshot and a summary of the config.
- The export includes integrity breakdown entries per (topic, subscription, producer) and stable JSON keys for easy diffing.
