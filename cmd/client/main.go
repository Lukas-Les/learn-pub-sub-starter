package main

import (
	"fmt"
	"github.com/Lukas-Les/learn-pub-sub-starter/internal/gamelogic"
	"github.com/Lukas-Les/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
	"os"
	"os/signal"
)

type SimpleQueueType = int

const (
	durable SimpleQueueType = iota
	transient
)

func main() {
	connectionString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Print(err.Error())
		return
	}
	defer conn.Close()
	fmt.Println("Connected to amqp")
	fmt.Println("Starting Peril client...")
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Print(err.Error())
		return
	}
	fmt.Printf("Hello, %s", username)
	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	_, _, err = DeclareAndBind(conn, "peril_direct", queueName, "pause", transient)
	if err != nil {
		fmt.Print(err.Error())
		return
	}
	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		fmt.Print(err.Error())
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	isDurable := queueType == durable
	queue, err := ch.QueueDeclare(queueName, isDurable, !isDurable, !isDurable, false, nil)
	if err != nil {
		fmt.Print(err.Error())
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		fmt.Print(err.Error())
		return &amqp.Channel{}, amqp.Queue{}, err
	}
	return ch, queue, nil
}
