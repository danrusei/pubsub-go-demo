package producer

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	danube "github.com/danube-messaging/danube-go"

	"github.com/danube-messaging/loadtest_danube/pkg/config"
	"github.com/danube-messaging/loadtest_danube/pkg/metrics"
	"github.com/danube-messaging/loadtest_danube/pkg/workload"
)

type Pool struct {
	serviceURL string
	cfg        *config.Config
	metrics    *metrics.Collector
	stopOnce   sync.Once
}

func NewPool(serviceURL string, cfg *config.Config, m *metrics.Collector) *Pool {
	return &Pool{serviceURL: serviceURL, cfg: cfg, metrics: m}
}

// Start launches producer workers for all producer groups in the config.
func (p *Pool) Start(ctx context.Context, wg *sync.WaitGroup) {
	// Single shared client for schema registration and all producers.
	client, err := danube.NewClient().ServiceURL(p.serviceURL).Build()
	if err != nil {
		log.Printf("client build error: %v", err)
		return
	}

	// Pre-register schemas once per topic (before launching workers).
	registered := map[string]bool{}
	for _, pg := range p.cfg.Producers {
		topicCfg := p.findTopic(pg.Topic)
		if topicCfg == nil || topicCfg.Schema == nil || !topicCfg.Schema.NeedsRegistration() {
			continue
		}
		subject := topicCfg.SchemaSubject()
		if registered[subject] {
			continue
		}
		if err := registerSchema(ctx, client, topicCfg); err != nil {
			log.Printf("schema register error for %s: %v", subject, err)
			continue
		}
		log.Printf("schema registered: subject=%s type=%s", subject, topicCfg.Schema.Type)
		registered[subject] = true
	}

	for _, pg := range p.cfg.Producers {
		topicCfg := p.findTopic(pg.Topic)
		for i := 0; i < pg.Count; i++ {
			wg.Add(1)
			go func(group config.ProducerGroup, tc *config.Topic, workerIdx int) {
				defer wg.Done()
				p.runWorker(ctx, client, group, tc, workerIdx)
			}(pg, topicCfg, i)
		}
	}
}

func (p *Pool) findTopic(name string) *config.Topic {
	for i := range p.cfg.Topics {
		if p.cfg.Topics[i].Name == name {
			return &p.cfg.Topics[i]
		}
	}
	return nil
}

func (p *Pool) runWorker(ctx context.Context, client *danube.DanubeClient, pg config.ProducerGroup, topicCfg *config.Topic, idx int) {
	baseName := pg.Name
	if baseName == "" {
		baseName = "producer"
	}
	prodName := fmt.Sprintf("%s-%d", baseName, idx)

	builder := client.NewProducer().
		WithName(prodName).
		WithTopic(pg.Topic)

	if topicCfg != nil {
		if topicCfg.Partitions > 0 {
			builder = builder.WithPartitions(int32(topicCfg.Partitions))
		}
		if topicCfg.DispatchStrategy == "reliable" {
			builder = builder.WithDispatchStrategy(danube.NewReliableDispatchStrategy())
		}
	}

	producer, err := builder.Build()
	if err != nil {
		log.Printf("producer build error: %v", err)
		p.metrics.IncError(1)
		return
	}
	if err := producer.Create(ctx); err != nil {
		log.Printf("producer create error: %v", err)
		p.metrics.IncError(1)
		return
	}

	// rate limiter per worker
	r := pg.RatePerSecond
	var limiter *rate.Limiter
	if r > 0 {
		limiter = rate.NewLimiter(rate.Limit(r), r)
	}

	payloadType := "string"
	if topicCfg != nil {
		payloadType = topicCfg.PayloadType()
	}
	pspec := workload.PayloadSpec{SchemaType: payloadType, MessageSize: pg.MessageSize}

	var seq uint64
	for {
		if ctx.Err() != nil {
			return
		}
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return
			}
		}
		seq++
		payload := workload.GeneratePayload(pspec, seq)
		attrs := map[string]string{
			"seq":      fmt.Sprintf("%d", seq),
			"producer": prodName,
		}
		if _, err := producer.Send(ctx, payload, attrs); err != nil {
			p.metrics.IncError(1)
			log.Printf("send error topic=%s worker=%d: %v", pg.Topic, idx, err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		p.metrics.IncSent(1)
	}
}

// registerSchema registers the topic's schema in the schema registry.
func registerSchema(ctx context.Context, client *danube.DanubeClient, topicCfg *config.Topic) error {
	schemaType, err := danube.ParseSchemaType(topicCfg.Schema.Type)
	if err != nil {
		return fmt.Errorf("invalid schema type %q: %w", topicCfg.Schema.Type, err)
	}
	builder := client.Schema().RegisterSchema(topicCfg.SchemaSubject()).
		WithType(schemaType).
		WithDescription("loadtest auto-registered")
	if topicCfg.Schema.Definition != "" {
		builder = builder.WithSchemaData([]byte(strings.TrimSpace(topicCfg.Schema.Definition)))
	}
	_, err = builder.Execute(ctx)
	return err
}
