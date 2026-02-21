package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/Lukas-Les/learn-pub-sub-starter/internal/gamelogic"
	"github.com/Lukas-Les/learn-pub-sub-starter/internal/pubsub"
	"github.com/Lukas-Les/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game client connected to RabbitMQ!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not get username: %v", err)
	}

	gs := gamelogic.NewGameState(username)
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+username,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gs),
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("army_moves.%s", username),
		"army_moves.*",
		pubsub.SimpleQueueTransient,
		handlerMove(ch, gs),
	)
	if err != nil {
		log.Fatalf("could not subscribe to army_moves: %v", err)
	}
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.SimpleQueueDurable,
		handlerWar(ch, gs),
	)
	if err != nil {
		log.Fatalf("could not subscribe to war: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "move":
			mv, err := gs.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
				mv,
			)
			if err != nil {
				log.Printf("could not publish time: %v", err)
			} else {
				log.Println("move published successfully")
			}

		case "spawn":
			err = gs.CommandSpawn(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(words) != 2 {
				log.Println("usage: spam <int>")
				continue
			}
			n, err := strconv.Atoi(words[1])
			if err != nil {
				log.Println("usage: spam <int>")
				continue
			}
			for range n {
				msg := gamelogic.GetMaliciousLog()
				gl := routing.GameLog{CurrentTime: time.Now(), Message: msg, Username: username}
				pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+username, gl)
			}
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("unknown command")
		}
	}
}
