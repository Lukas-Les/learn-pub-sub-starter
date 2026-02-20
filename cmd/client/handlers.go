package main

import (
	"fmt"
	"time"

	"github.com/Lukas-Les/learn-pub-sub-starter/internal/gamelogic"
	"github.com/Lukas-Les/learn-pub-sub-starter/internal/pubsub"
	"github.com/Lukas-Les/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(ps routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(ch *amqp.Channel, gs *gamelogic.GameState) func(move gamelogic.ArmyMove) pubsub.Acktype {
	return func(move gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		mvOutcome := gs.HandleMove(move)
		switch mvOutcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.Player.Username), gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		return pubsub.NackDiscard
	}
}

func buildAndPublishGameLog(ch *amqp.Channel, msg, username string) error {
	gl := routing.GameLog{CurrentTime: time.Now(), Message: msg, Username: username}
	return pubsub.PublishGameLog(ch, gl)
}

func handlerWar(ch *amqp.Channel, gs *gamelogic.GameState) func(rw gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(rw gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw) // winner, loser
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			err := buildAndPublishGameLog(ch, fmt.Sprintf("%s won a war against %s", winner, loser), rw.Attacker.Username)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			err := buildAndPublishGameLog(ch, fmt.Sprintf("%s won a war against %s", winner, loser), rw.Attacker.Username)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			err := buildAndPublishGameLog(ch, fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser), rw.Attacker.Username)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Println("unknown outcome")
			return pubsub.NackDiscard
		}
		return pubsub.Ack
	}
}
