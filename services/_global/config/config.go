package config

type Config struct {
	App           string
	Instance      string
	RedisHostname string
	RedisPort     uint16
}

var g_config *Config

func GetConfig() *Config {
	if g_config == nil {
		// TODO: Use Viper module for config management
		g_config = &Config{App: "EVAN_TEST", Instance: "adder1", RedisHostname: "localhost", RedisPort: 6379}
	}

	return g_config
}
