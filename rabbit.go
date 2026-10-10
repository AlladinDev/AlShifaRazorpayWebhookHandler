package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitConfig struct {
	URL        string // full AMQP URL; if empty it is built from the parts below
	Host       string
	Port       string
	User       string
	Password   string
	VHost      string
	TLS        bool
	Exchange   string
	Kind       string // direct | topic | fanout | headers
	RoutingKey string
	Queue      string // if set, the queue is declared and bound to the exchange
}

func loadRabbitConfig() rabbitConfig {
	return rabbitConfig{
		URL:        os.Getenv("RABBITMQ_URL"),
		Host:       getenv("RABBITMQ_HOST", "localhost"),
		Port:       getenv("RABBITMQ_PORT", ""),
		User:       getenv("RABBITMQ_USER", "guest"),
		Password:   getenv("RABBITMQ_PASSWORD", "guest"),
		VHost:      getenv("RABBITMQ_VHOST", "/"),
		TLS:        strings.EqualFold(os.Getenv("RABBITMQ_TLS"), "true"),
		Exchange:   getenv("RABBITMQ_EXCHANGE", "alshifa.payments"),
		Kind:       getenv("RABBITMQ_EXCHANGE_TYPE", "direct"),
		RoutingKey: getenv("RABBITMQ_ROUTING_KEY", "payment.webhook"),
		Queue:      getenv("RABBITMQ_QUEUE", "alshifa.payment.webhooks"),
	}
}

func (c rabbitConfig) dialURL() string {
	if c.URL != "" {
		return c.URL
	}
	scheme, port := "amqp", c.Port
	if c.TLS {
		scheme = "amqps"
	}
	if port == "" {
		port = "5672"
		if c.TLS {
			port = "5671"
		}
	}
	// url.URL handles escaping of special characters in user/password/vhost.
	u := url.URL{
		Scheme: scheme,
		User:   url.UserPassword(c.User, c.Password),
		Host:   c.Host + ":" + port,
		Path:   "/" + c.VHost,
	}
	if c.VHost == "/" {
		u.Path = "/"
		u.RawPath = "/%2F"
	} else {
		u.RawPath = "/" + url.PathEscape(c.VHost)
	}
	return u.String()
}

type publisher struct {
	cfg  rabbitConfig
	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
}

func newPublisher(cfg rabbitConfig) *publisher { return &publisher{cfg: cfg} }

// connect (re)establishes connection, channel, topology and publisher confirms.
// Caller must hold p.mu or be the only user (startup).
func (p *publisher) connect() error {
	p.closeLocked()

	conn, err := amqp.DialConfig(p.cfg.dialURL(), amqp.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
		Dial:      amqp.DefaultDial(5 * time.Second),
	})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("channel: %w", err)
	}

	if p.cfg.Exchange != "" {
		if err := ch.ExchangeDeclare(p.cfg.Exchange, p.cfg.Kind, true, false, false, false, nil); err != nil {
			conn.Close()
			return fmt.Errorf("exchange declare: %w", err)
		}
		if p.cfg.Queue != "" {
			if _, err := ch.QueueDeclare(p.cfg.Queue, true, false, false, false, nil); err != nil {
				conn.Close()
				return fmt.Errorf("queue declare: %w", err)
			}
			if err := ch.QueueBind(p.cfg.Queue, p.cfg.RoutingKey, p.cfg.Exchange, false, nil); err != nil {
				conn.Close()
				return fmt.Errorf("queue bind: %w", err)
			}
		}
	}

	// Publisher confirms: the broker acks only after it has accepted (and, for durable queues, persisted) the message.
	if err := ch.Confirm(false); err != nil {
		conn.Close()
		return fmt.Errorf("confirm mode: %w", err)
	}

	p.conn, p.ch = conn, ch
	log.Printf("connected to RabbitMQ (exchange=%q queue=%q routing_key=%q)", p.cfg.Exchange, p.cfg.Queue, p.cfg.RoutingKey)
	return nil
}

func (p *publisher) closeLocked() {
	if p.ch != nil {
		_ = p.ch.Close()
		p.ch = nil
	}
	if p.conn != nil && !p.conn.IsClosed() {
		_ = p.conn.Close()
	}
	p.conn = nil
}

func (p *publisher) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closeLocked()
}

// publish sends body unchanged and waits for the broker's confirm.
// On a connection problem it reconnects once and retries.
func (p *publisher) publish(ctx context.Context, body []byte, contentType, messageID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastErr error
	for range 2 {
		if p.conn == nil || p.conn.IsClosed() || p.ch == nil || p.ch.IsClosed() {
			if err := p.connect(); err != nil {
				lastErr = err
				continue
			}
		}
		err := p.publishOnce(ctx, body, contentType, messageID)
		if err == nil {
			return nil
		}
		lastErr = err
		p.closeLocked() // force a fresh connection on the retry
	}
	return lastErr
}

func (p *publisher) publishOnce(ctx context.Context, body []byte, contentType, messageID string) error {
	conf, err := p.ch.PublishWithDeferredConfirmWithContext(ctx,
		p.cfg.Exchange, p.cfg.RoutingKey,
		true,  // mandatory: error if the message can't be routed to any queue
		false, // immediate (unsupported by RabbitMQ)
		amqp.Publishing{
			ContentType:  contentType,
			DeliveryMode: amqp.Persistent,
			MessageId:    messageID, // Razorpay event id, lets consumers de-duplicate
			Timestamp:    time.Now(),
			Body:         body, // raw bytes, untouched
		})
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	acked, err := conf.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("waiting for confirm: %w", err)
	}
	if !acked {
		return errors.New("broker nacked the message")
	}
	return nil
}
