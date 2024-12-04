package main

import (
	"context"
	"fmt"
	"log"

	darkmq "github.com/sagleft/darkrmq"
	"github.com/streadway/amqp"
)

// Consumer implement rabbitroutine.Consumer interface.
type Consumer struct {
	ExchangeName string
	QueueName    string
	Tag          string
}

// Declare implement rabbitroutine.Consumer.(Declare) interface method.
func (c *Consumer) Declare(ctx context.Context, ch *amqp.Channel) error {
	err := ch.ExchangeDeclare(
		c.ExchangeName, // name
		"direct",       // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		log.Printf("failed to declare exchange %v: %v", c.ExchangeName, err)

		return err
	}

	_, err = ch.QueueDeclare(
		c.QueueName, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,
	)
	if err != nil {
		log.Printf("failed to declare queue %v: %v", c.QueueName, err)

		return err
	}

	err = ch.QueueBind(
		c.QueueName,    // queue name
		c.QueueName,    // routing key
		c.ExchangeName, // exchange
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		log.Printf("failed to bind queue %v: %v", c.QueueName, err)

		return err
	}

	return nil
}

// Consume implement rabbitroutine.Consumer.(Consume) interface method.
func (c *Consumer) Consume(payload darkmq.ConsumeTask) error {
	defer log.Println("consume method finished")

	err := payload.Ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Printf("failed to set qos: %v", err)

		return err
	}

	msgs, err := payload.Ch.Consume(
		c.QueueName,  // queue
		"myconsumer", // consumer name
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		log.Printf("failed to consume %v: %v", c.QueueName, err)

		return err
	}

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return amqp.ErrClosed
			}

			content := string(msg.Body)

			fmt.Println("New message:", content)

			err := msg.Ack(false)
			if err != nil {
				log.Printf("failed to Ack message: %v", err)
			}
		case <-payload.Ctx.Done():
			return payload.Ctx.Err()
		}
	}
}

// GetTag - get consumer tag
func (c *Consumer) GetTag() string {
	return c.Tag
}

func (c *Consumer) ErrorCallback(err error) {
	//
}
