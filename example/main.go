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
	sm.Subscribe("ch:1", "a.*", func(msg *redis.Message) {
		log.Info(
			fmt.Sprintf(
				"['%v' '%v']: %v",
				msg.Channel,
				msg.Pattern,
				msg.Payload,
			),
		)
	})

	go func() {
		for i := 1; i <= 10; i++ {
			// Publish a message.
			time.Sleep(time.Second)
			msg := fmt.Sprintf("hello %v", i)
			log.Info("publish message: ", msg)
			err := redisClient.Publish("a.1", msg).Err()
			if err != nil {
				panic(err)
			}
		}
	}()

	for {
		time.Sleep(time.Second)
	}

}
