package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	marshalled, err := json.Marshal(val)
	if err != nil {
		return err
	}

	payload := amqp.Publishing{
		ContentType: "application/json",
		Body:        marshalled,
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, payload)
	if err != nil {
		return err
	}
	return nil
}
