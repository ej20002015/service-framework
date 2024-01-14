package dictionaries

import (
	"context"
	"svcframework/redisclient"

	"github.com/redis/go-redis/v9"
)

type RedisDictionary struct {
	Dictionary string
	Client     *redis.Client
}

func NewRedisDictionary(dictionaryName string) Dictionary {
	return &RedisDictionary{Dictionary: dictionaryName, Client: redisclient.RedisClient()}
}

func NewRedisDictionaryFromMap(dictionaryName string, mp map[string]string) Dictionary {
	dict := &RedisDictionary{Dictionary: dictionaryName, Client: redisclient.RedisClient()}
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
	return redisDictionary.Client.HSet(ctx, redisDictionary.Dictionary, key, val).Err()
}

func (redisDictionary *RedisDictionary) Get(key string) (string, error) {
	ctx := context.Background()
	return redisDictionary.Client.HGet(ctx, redisDictionary.Dictionary, key).Result()
}

func (redisDictionary *RedisDictionary) Delete(key string) error {
	ctx := context.Background()
	return redisDictionary.Client.HDel(ctx, redisDictionary.Dictionary, key).Err()
}
