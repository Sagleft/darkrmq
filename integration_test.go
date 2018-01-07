// +build integration

package rabbitroutine

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

var testCfg = integrationConfig()

type FakeConsumer struct {
	declareFn func(ctx context.Context, ch *amqp.Channel) error
	consumeFn func(ctx context.Context, ch *amqp.Channel) error
}

func (c *FakeConsumer) Declare(ctx context.Context, ch *amqp.Channel) error {
	return c.declareFn(ctx, ch)
}

func (c *FakeConsumer) Consume(ctx context.Context, ch *amqp.Channel) error {
	return c.consumeFn(ctx, ch)
}

func TestIntegrationEnsurePublish(t *testing.T) {
	ctx := context.Background()

	conn := NewConnector(testCfg)

	go func() {
		err := conn.Start(ctx)
		assert.NoError(t, err)
	}()

	ch, err := conn.Channel(ctx)
	assert.NoError(t, err, "failed to receive channel")
	assert.NotNil(t, ch, "nil channel received")
	defer ch.Close()

	testExchange := "TestIntegrationEnsurePublish_Exchange"
	testQueue := "TestIntegrationEnsurePublish_Queue"
	testMsg := "TestIntegrationEnsurePublish"

	err = ch.ExchangeDeclare(testExchange, "direct", false, true, false, false, nil)
	assert.NoError(t, err, "failed to declare exchange")

	pool := NewPool(conn)
	p := NewPublisher(pool)
	err = p.EnsurePublish(ctx, testExchange, testQueue, amqp.Publishing{Body: []byte(testMsg)})
	assert.NoError(t, err, "failed to publish")
}

func TestIntegrationEnsurePublishedReceiving(t *testing.T) {
	ctx := context.Background()

	conn := NewConnector(testCfg)
	pool := NewPool(conn)
	pub := NewPublisher(pool)

	testExchange := "TestIntegrationEnsurePublishedReceiving_Exchange"
	testQueue := "TestIntegrationEnsurePublishedReceiving_Queue"
	testMsg := "TestIntegrationEnsurePublishedReceiving"

	deliveriesCh := make(chan string)

	consumer := &FakeConsumer{
		declareFn: func(ctx context.Context, ch *amqp.Channel) error {
			return nil
		},
		consumeFn: func(ctx context.Context, ch *amqp.Channel) error {
			err := ch.Qos(1, 0, false)
			assert.NoError(t, err)

			msgs, err := ch.Consume(testQueue, "test_consumer", true, false, false, false, nil)
			assert.NoError(t, err)

			msg := <-msgs

			content := string(msg.Body)

			deliveriesCh <- content

			return nil
		},
	}

	go func() {
		err := conn.Start(ctx)
		assert.NoError(t, err)
	}()

	ch, err := conn.Channel(ctx)
	assert.NoError(t, err, "failed to receive channel")
	assert.NotNil(t, ch, "nil channel received")
	defer ch.Close()

	err = ch.ExchangeDeclare(testExchange, "direct", false, true, false, false, nil)
	assert.NoError(t, err, "failed to declare exchange")

	_, err = ch.QueueDeclare(testQueue, false, false, false, false, nil)
	assert.NoError(t, err)

	err = ch.QueueBind(testQueue, testQueue, testExchange, false, nil)
	assert.NoError(t, err)

	err = pub.EnsurePublish(ctx, testExchange, testQueue, amqp.Publishing{Body: []byte(testMsg)})
	assert.NoError(t, err)

	go func() {
		err := conn.StartConsumer(ctx, consumer)
		assert.NoError(t, err)
	}()

	actualMsg := <-deliveriesCh
	assert.Equal(t, testMsg, actualMsg)
}

func TestIntegrationConcurrentPublishing(t *testing.T) {
	ctx := context.Background()

	conn := NewConnector(testCfg)
	pool := NewPool(conn)
	pub := NewPublisher(pool)

	go func() {
		err := conn.Start(ctx)
		assert.NoError(t, err)
	}()

	testExchange := "TestIntegrationConcurrentPublishing_Exchange"
	testQueue := "TestIntegrationConcurrentPublishing_Queue"
	testMsg := "TestIntegrationConcurrentPublishing"

	ch, err := conn.Channel(ctx)
	assert.NoError(t, err, "failed to receive channel")
	assert.NotNil(t, ch, "nil channel received")
	defer ch.Close()

	err = ch.ExchangeDeclare(testExchange, "direct", false, true, false, false, nil)
	assert.NoError(t, err, "failed to declare exchange")

	// Number of goroutine for concurrent publishing
	N := 2

	var wg sync.WaitGroup
	wg.Add(N)

	for i := 0; i < N; i++ {
		go func() {
			err := pub.EnsurePublish(ctx, testExchange, testQueue, amqp.Publishing{Body: []byte(testMsg)})
			assert.NoError(t, err)

			wg.Done()
		}()
	}

	wg.Wait()
}

func integrationURLFromEnv() string {
	url := os.Getenv("AMQP_URL")
	if url == "" {
		url = "amqp://"
	}

	return url
}

func integrationConfig() Config {
	uri, err := amqp.ParseURI(integrationURLFromEnv())
	if err != nil {
		panic("failed to parse AMQP_URL")
	}

	return Config{
		Host:     uri.Host,
		Port:     uri.Port,
		Username: uri.Username,
		Password: uri.Password,
		Attempts: 20000,
		Wait:     5 * time.Second,
	}
}