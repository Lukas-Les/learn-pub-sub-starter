package main

import (
	"fmt"
	"log"

	"github.com/Lukas-Les/learn-pub-sub-starter/internal/gamelogic"
	"github.com/Lukas-Les/learn-pub-sub-starter/internal/pubsub"

	"github.com/Lukas-Les/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game server connected to RabbitMQ!")

	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	gamelogic.PrintServerHelp()
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		if input[0] == "pause" {
			fmt.Println("Sending a pause message")
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			if err != nil {
				log.Printf("could not publish time: %v", err)
			}
			fmt.Println("Pause message sent!")
		} else if input[0] == "resume" {
			fmt.Println("Sending a pause message")
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				log.Printf("could not publish time: %v", err)
			}
			fmt.Println("Unpause message sent!")
		} else if input[0] == "quit" {
			fmt.Println("Quitting!")
			break
		} else {
			log.Printf("no such command: %s", input[0])
		}
	}
}
