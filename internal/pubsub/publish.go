package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/Lukas-Les/learn-pub-sub-starter/internal/routing"
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
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, payload)
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	fmt.Println("encoding a game log")
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(val)
	if err != nil {
		return err
	}
	payload := amqp.Publishing{
		ContentType: "application/gob",
		Body:        buff.Bytes(),
	}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, payload)
}

func PublishGameLog(ch *amqp.Channel, gl routing.GameLog) error {
	fmt.Println("publishing a game log")
	return PublishGob(ch, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.GameLogSlug, gl.Username), gl)
}
