package main

import (
	"fmt"
	"github.com/go-redis/redis"
	log "github.com/sirupsen/logrus"
	"github.com/vskrachkov/pubsub/src"
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

	pattern := "a.*"
	pattern2 := "*"

	channel1 := "ch:1"
	sm.Subscribe(&pattern, newConsumer(&channel1))

	channel2 := "ch:2"
	sm.Subscribe(&pattern, newConsumer(&channel2))

	channel3 := "ch:3"
	sm.Subscribe(&pattern2, newConsumer(&channel3))

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
	return func(msg *redis.Message) {
		log.Info(
			fmt.Sprintf(
				"[%d] '%s' received message: ['%s' '%s']: %s",
				name,
				*name,
				msg.Channel,
				msg.Pattern,
				msg.Payload,
			),
		)
	}
}
