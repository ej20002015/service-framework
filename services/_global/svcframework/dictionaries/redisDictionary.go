package dictionaries

import (
	"context"
	"svcframework/redisclient"

	"github.com/redis/go-redis/v9"
)

type RedisDictionary struct {
	Dictionary string
	client     *redis.Client
}

func NewRedisDictionary(dictionaryName string) Dictionary {
	return &RedisDictionary{Dictionary: dictionaryName, client: redisclient.RedisClient()}
}

func NewRedisDictionaryFromMap(dictionaryName string, mp map[string]string) Dictionary {
	dict := &RedisDictionary{Dictionary: dictionaryName, client: redisclient.RedisClient()}
	for k, v := range mp {
		dict.Set(k, v)
	}

	return dict
}

func (redisDictionary *RedisDictionary) Identifier() string {
	return redisDictionary.Dictionary
}

func (redisDictionary *RedisDictionary) Set(key string, val string) error {
	ctx := context.Background()
	return redisDictionary.client.HSet(ctx, redisDictionary.Dictionary, key, val).Err()
}

func (redisDictionary *RedisDictionary) Get(key string) (string, error) {
	ctx := context.Background()
	return redisDictionary.client.HGet(ctx, redisDictionary.Dictionary, key).Result()
}

func (redisDictionary *RedisDictionary) Delete(key string) error {
	ctx := context.Background()
	return redisDictionary.client.HDel(ctx, redisDictionary.Dictionary, key).Err()
}
