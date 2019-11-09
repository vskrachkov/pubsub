package pubsub

import (
	"github.com/go-redis/redis"
	log "github.com/sirupsen/logrus"
)

type Consumer = func(message *redis.Message)

type SubscriptionManager struct {
	redisClient   *redis.Client
	subscriptions map[string]bool
	consumers     map[string]*[]Consumer
}

func NewSubscriptionManager(redisClient *redis.Client) *SubscriptionManager {
	return &SubscriptionManager{
		redisClient:   redisClient,
		subscriptions: map[string]bool{},
		consumers:     map[string]*[]Consumer{},
	}
}

func (sm *SubscriptionManager) Subscribe(
	pattern *string,
	consumer Consumer,
) {
	log.Info("subscribing to the pattern: ", *pattern)
	if sm.consumers[*pattern] == nil {
		sm.consumers[*pattern] = &[]Consumer{consumer}
		sm.subscribe(pattern)
	} else {
		*sm.consumers[*pattern] = append(*sm.consumers[*pattern], consumer)
	}
}

func (sm SubscriptionManager) subscribe(pattern *string) {
	if !sm.subscriptions[*pattern] {
		subscription := sm.redisClient.PSubscribe(*pattern)
		_, err := subscription.Receive() // Wait for confirmation that subscription is created
		if err != nil {
			panic(err)
		}
		sm.subscriptions[*pattern] = true
		ch := subscription.Channel()
		go func() {
			for msg := range ch {
				sm.consume(msg)
			}
		}()
	}
}

func (sm SubscriptionManager) consume(message *redis.Message) {
	consumers := sm.consumers[message.Pattern]
	for _, consumer := range *consumers {
		if consumer != nil {
			consumer(message)
		}
	}
}
