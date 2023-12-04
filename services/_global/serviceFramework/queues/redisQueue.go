package queues

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

var g_redisClient *redis.Client

type RedisQueue struct {
	Queue  string
	client *redis.Client
}

func NewRedisQueue(queueName string) Queue {
	if g_redisClient == nil {
		g_redisClient = redis.NewClient(&redis.Options{
			Addr:     "localhost:6379",
			Password: "", // no password set
			DB:       0,  // use default DB
		})
	}

	return &RedisQueue{Queue: queueName, client: g_redisClient}
}

func (redisQueue *RedisQueue) Push(val string) error {
	ctx := context.Background()
	return redisQueue.client.LPush(ctx, redisQueue.Queue, val).Err()
}

func (redisQueue *RedisQueue) Pop() (string, error) {
	ctx := context.Background()
	return redisQueue.client.RPop(ctx, redisQueue.Queue).Result()
}

func (redisQueue *RedisQueue) BlockingPop(timeout time.Duration) (string, error) {
	ctx := context.Background()
	val, err := redisQueue.client.BLPop(ctx, timeout, redisQueue.Queue).Result()
	return val[0], err
}

func (redisQueue *RedisQueue) Peek() (string, error) {
	ctx := context.Background()
	val, err := redisQueue.client.LRange(ctx, redisQueue.Queue, -1, -1).Result()
	return val[0], err
}
