package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create channel: %v", err)
	}

	queeArgs := amqp.Table{"x-dead-letter-exchange": "peril_dlx"}
	queue, err := ch.QueueDeclare(
		queueName,                       // name
		queueType == SimpleQueueDurable, // durable
		queueType != SimpleQueueDurable, // delete when unused
		queueType != SimpleQueueDurable, // exclusive
		false,                           // no-wait
		queeArgs,                        // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare queue: %v", err)
	}

	err = ch.QueueBind(
		queue.Name, // queue name
		key,        // routing key
		exchange,   // exchange
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not bind queue: %v", err)
	}
	return ch, queue, nil
}

func handleDeliveryChannel[T any](delivs <-chan amqp.Delivery, handler func(T) Acktype) {
	for d := range delivs {
		var decoded T
		json.Unmarshal(d.Body, &decoded)
		ack := handler(decoded)
		switch ack {
		case Ack:
			fmt.Println("ack")
			d.Ack(false)
		case NackRequeue:
			fmt.Println("nack requeue")
			d.Nack(false, true)
		case NackDiscard:
			fmt.Println("nack discard")
			d.Nack(false, false)
		}
	}
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) Acktype,
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	delCh, err := ch.ConsumeWithContext(context.Background(), queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go handleDeliveryChannel(delCh, handler)
	return nil
}
