package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
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

func handleDeliveryChannel[T any](delivs <-chan amqp.Delivery, handler func(T) Acktype, unmarshaller func([]byte) (T, error)) {
	for d := range delivs {
		unmarshalled, err := unmarshaller(d.Body)
		if err != nil {
			fmt.Printf("failed to decode: %s", err.Error())
			d.Nack(false, false)
			continue
		}
		ack := handler(unmarshalled)
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

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}
	ch.Qos(10, 0, false)
	delCh, err := ch.ConsumeWithContext(context.Background(), queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go handleDeliveryChannel(delCh, handler, unmarshaller)
	return nil

}

func unmarshallJSON[T any](b []byte) (T, error) {
	var result T
	err := json.Unmarshal(b, &result)
	return result, err
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) Acktype,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshallJSON)
}

func unmarshallGob[T any](b []byte) (T, error) {
	var target T
	buff := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buff)
	err := dec.Decode(&target)
	return target, err
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) Acktype,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshallGob)
}
