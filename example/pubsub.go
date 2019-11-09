package main

import (
	"fmt"
	"github.com/go-redis/redis"
	log "github.com/sirupsen/logrus"
	"github.com/vskrachkov/pubsub/src"
	"math/rand"
	"time"
)

func init() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	log.SetLevel(log.InfoLevel)
}

func main() {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	sm := src.NewSubscriptionManager(redisClient)

	pattern1 := "a.*"
	sm.Subscribe(&pattern1, newConsumer(&pattern1))
	sm.Subscribe(&pattern1, newConsumer(&pattern1))

	// Publish a messages
	for i := 1; i <= 10; i++ {
		time.Sleep(time.Second)
		msg := fmt.Sprintf("hello %v", i)
		log.Info("publish message: ", msg)
		err := redisClient.Publish("a.1", msg).Err()
		if err != nil {
			panic(err)
		}
	}

}

func newConsumer(name *string) func(message *redis.Message) {
	id := rand.Intn(100)
	return func(msg *redis.Message) {
		log.Info(
			fmt.Sprintf(
				"{%d}{%s} received message: ['%s' '%s']: %s",
				id,
				*name,
				msg.Channel,
				msg.Pattern,
				msg.Payload,
			),
		)
	}
}
