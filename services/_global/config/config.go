package config

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func Init() {
	// Read config from file

	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error reading config file: %w", err))
	}

	// Read config from cmd line

	pflag.String("AppName", "EVAN_TEST", "Name of application")
	pflag.String("Instance", "test-instance", "Name of server instance")
	pflag.String("RedisHostname", "localhost", "Hostname of redis server")
	pflag.Uint16("RedisPort", 6379, "Port of redis server")
	pflag.Uint32("MaxRuns", 3, "Max number of times to attempt to run a job")
	pflag.Duration("Delay", time.Second*2, "Number of seconds between each job rerun")
	pflag.Float32("DelayFactor", 2.0, "Factor used to increase wait time for each subsequent job rerun - set as 1 for every re-run to be a constant delay time as set by Delay")
	pflag.Int("NumWorkerThreads", 4, "Number of worker threads to spawn")

	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	config := viper.AllSettings()
	fmt.Printf("Config: %+v\n\n", config)
}
