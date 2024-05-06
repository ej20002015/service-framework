package redisclient

import (
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

var g_redisClient *redis.Client

func RedisClient() *redis.Client {
	if g_redisClient == nil {
		g_redisClient = redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", viper.GetString("RedisHostname"), viper.GetUint16("RedisPort")),
			Password: "", // no password set
			DB:       0,  // use default DB
		}) // TODO: Error checking
	}

	return g_redisClient
}
