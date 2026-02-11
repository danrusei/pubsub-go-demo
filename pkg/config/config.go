package config

// Core configuration structures for load testing scenarios.

type Config struct {
	TestName    string `yaml:"test_name"`
	Description string `yaml:"description,omitempty"`

	Danube    DanubeConfig    `yaml:"danube"`
	Execution ExecutionConfig `yaml:"execution"`

	Topics    []Topic         `yaml:"topics"`
	Producers []ProducerGroup `yaml:"producers"`
	Consumers []ConsumerGroup `yaml:"consumers"`

	Metrics MetricsConfig `yaml:"metrics"`
}

type DanubeConfig struct {
	ServiceURL        string `yaml:"service_url"`
	ConnectionTimeout string `yaml:"connection_timeout,omitempty"`
}

type ExecutionConfig struct {
	Duration         string `yaml:"duration"`
	WarmupDuration   string `yaml:"warmup_duration,omitempty"`
	CooldownDuration string `yaml:"cooldown_duration,omitempty"`
}

type Topic struct {
	Name             string        `yaml:"name"`
	Partitions       int           `yaml:"partitions"`                  // 0 or omitted means non-partitioned
	DispatchStrategy string        `yaml:"dispatch_strategy,omitempty"` // reliable|non_reliable (default non_reliable)
	Schema           *SchemaConfig `yaml:"schema,omitempty"`            // optional schema registry config
}

// SchemaConfig describes a schema to register and attach to the producer.
type SchemaConfig struct {
	Subject    string `yaml:"subject,omitempty"`    // schema registry subject; defaults to "<topic>-value"
	Type       string `yaml:"type"`                 // json_schema|string|number|bytes|avro|protobuf
	Definition string `yaml:"definition,omitempty"` // required for json_schema|avro|protobuf
}

// PayloadType returns the workload generator key derived from the schema config.
func (t *Topic) PayloadType() string {
	if t.Schema == nil {
		return "string"
	}
	switch t.Schema.Type {
	case "json_schema":
		return "json"
	case "number":
		return "number"
	case "string", "bytes", "":
		return "string"
	default:
		return "string"
	}
}

// SchemaSubject returns the subject name, defaulting to "<topic>-value".
func (t *Topic) SchemaSubject() string {
	if t.Schema != nil && t.Schema.Subject != "" {
		return t.Schema.Subject
	}
	return t.Name + "-value"
}

// NeedsRegistration returns true if the schema type requires registry registration
// (i.e. has a structured definition). Simple types like string/number/bytes are
// only payload hints for the workload generator and don't use the schema registry.
func (s *SchemaConfig) NeedsRegistration() bool {
	switch s.Type {
	case "json_schema", "avro", "protobuf":
		return true
	default:
		return false
	}
}

type ProducerGroup struct {
	Name          string `yaml:"name"`
	Topic         string `yaml:"topic"`
	Count         int    `yaml:"count"`
	RatePerSecond int    `yaml:"rate_per_second"`
	MessageSize   int    `yaml:"message_size"`
	BatchSize     int    `yaml:"batch_size,omitempty"`
}

type ConsumerGroup struct {
	Name             string `yaml:"name"`
	Topic            string `yaml:"topic"`
	Subscription     string `yaml:"subscription"`
	SubscriptionType string `yaml:"subscription_type"` // shared|exclusive|failover
	Count            int    `yaml:"count"`
	AckTimeout       string `yaml:"ack_timeout,omitempty"`
}

type MetricsConfig struct {
	Enabled        bool      `yaml:"enabled"`
	ReportInterval string    `yaml:"report_interval"`
	Percentiles    []float64 `yaml:"percentiles"`
	OutputFormat   string    `yaml:"output_format"` // terminal|json|prometheus
	ExportPath     string    `yaml:"export_path"`
	Collect        []string  `yaml:"collect"`
}
