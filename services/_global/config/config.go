package config

import "time"

type Config struct {
	App           string
	Instance      string
	RedisHostname string
	RedisPort     uint16
	MaxRuns       uint32
	Delay         time.Duration
	DelayFactor   float64
}

var g_config *Config

func GetConfig() *Config {
	if g_config == nil {
		// TODO: Use Viper module for config management
		g_config = &Config{
			App:           "EVAN_TEST",
			Instance:      "adder1",
			RedisHostname: "localhost",
			RedisPort:     6379,
			MaxRuns:       4,
			Delay:         time.Second * 2,
			DelayFactor:   2.0,
		}
	}

	return g_config
}
