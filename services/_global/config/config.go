package config

type Config struct {
	AppName       string
	RedisHostname string
	RedisPort     uint16
}

var g_config *Config

func GetConfig() *Config {
	if g_config == nil {
		// TODO: Use Viper module for config management
		g_config = &Config{AppName: "EVAN_TEST", RedisHostname: "localhost", RedisPort: 6379}
	}

	return g_config
}
