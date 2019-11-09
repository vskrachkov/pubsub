package src

import (
	"github.com/go-redis/redis"
	log "github.com/sirupsen/logrus"
)

type Consumer = func(message *redis.Message)

type SubscriptionManager struct {
	redisClient    *redis.Client
	pubSubs        map[string]*redis.PubSub
	consumers      map[string]*[]Consumer
	clientChannels map[string]*[]string
}

func NewSubscriptionManager(redisClient *redis.Client) *SubscriptionManager {
	return &SubscriptionManager{
		redisClient:    redisClient,
		pubSubs:        map[string]*redis.PubSub{},
		consumers:      map[string]*[]Consumer{},
		clientChannels: map[string]*[]string{},
	}
}

func (sm *SubscriptionManager) Subscribe(
	clientChannel string,
	pattern string,
	consumer Consumer,
) {
	log.Info("subscribing to the pattern: ", pattern)
	sm.updateConsumers(pattern, consumer)
	sm.updateClientChannels(pattern, clientChannel)
	ch := sm.subscribe(pattern) // Go channel which receives messages
	go func() {
		for msg := range ch {
			consumer(msg)
		}
	}()
}

func (sm SubscriptionManager) updateConsumers(
	pattern string,
	consumer Consumer,
) {
	if sm.consumers[pattern] != nil {
		cons := sm.consumers[pattern]
		*cons = append(*cons, consumer)
	} else {
		cons := make([]Consumer, 8)
		cons = append(cons, consumer)
		sm.consumers[pattern] = &cons
	}
}

func (sm SubscriptionManager) updateClientChannels(
	pattern string,
	clientChannel string,
) {
	if sm.clientChannels[pattern] != nil {
		chs := sm.clientChannels[pattern]
		*chs = append(*chs, clientChannel)
	} else {
		chs := make([]string, 8)
		chs = append(chs, clientChannel)
		sm.clientChannels[pattern] = &chs
	}
}

func (sm SubscriptionManager) subscribe(
	pattern string,
) <-chan *redis.Message {
	pubsub := sm.pubSubs[pattern]
	if pubsub == nil {
		pubsub := sm.redisClient.PSubscribe(pattern)
		_, err := pubsub.Receive() // Wait for confirmation that subscription is created
		if err != nil {
			panic(err)
		}
		sm.pubSubs[pattern] = pubsub
	}
	return sm.pubSubs[pattern].Channel()
}
