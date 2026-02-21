package main

import (
	"fmt"

	"github.com/Lukas-Les/learn-pub-sub-starter/internal/gamelogic"
	"github.com/Lukas-Les/learn-pub-sub-starter/internal/pubsub"
	"github.com/Lukas-Les/learn-pub-sub-starter/internal/routing"
)

func handlerGameLog() func(gl routing.GameLog) pubsub.Acktype {
	return func(gl routing.GameLog) pubsub.Acktype {
		defer fmt.Print("> ")
		gamelogic.WriteLog(gl)
		return pubsub.Ack
	}
}
