package queues

import (
	"context"
	"svcframework/redisclient"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisQueue struct {
	Queue  string
	Client *redis.Client
}

func NewRedisQueue(queueName string) Queue {
	return &RedisQueue{Queue: queueName, Client: redisclient.RedisClient()}
}

func (redisQueue *RedisQueue) Push(val string) error {
	ctx := context.Background()
	return redisQueue.Client.LPush(ctx, redisQueue.Queue, val).Err()
}

func (redisQueue *RedisQueue) Pop() (string, error) {
	ctx := context.Background()
	return redisQueue.Client.RPop(ctx, redisQueue.Queue).Result()
}

func (redisQueue *RedisQueue) BlockingPop(timeout time.Duration) (string, error) {
	ctx := context.Background()
	val, err := redisQueue.Client.BLPop(ctx, timeout, redisQueue.Queue).Result()
	if err != nil {
		return "", err
	}
	return val[1], err
}

func (redisQueue *RedisQueue) Peek() (string, error) {
	ctx := context.Background()
	val, err := redisQueue.Client.LRange(ctx, redisQueue.Queue, -1, -1).Result()
	return val[0], err
}
