package logger

import (
	"config"
	"fmt"
	"os"
	"svcframework/queues"
	"svcframework/redisclient"

	"github.com/rs/zerolog"
)

type StdLogger = zerolog.Logger
type RedisLogger = zerolog.Logger

var g_logFile *os.File
var Logger StdLogger

func Init() {
	filepath := config.GetConfig().Instance + ".log"
	standardWriter, err := NewStdOutAndFileWriter(filepath)
	g_logFile = standardWriter.file
	if err != nil {
		fmt.Println("failed to initialise the logger")
		panic(err)
	}

	Logger = zerolog.New(*standardWriter).With().Timestamp().Logger()
}

func Shutdown() {
	g_logFile.Close()
}

func NewRedisLogger(queueName string) RedisLogger {
	return zerolog.New(NewStandardRedisWriter(queueName)).With().Timestamp().Logger()
}

type StdOutFileAndRedisWriter struct {
	stdOutAndFileWriter *StdOutAndFileWriter
	redisWriter         *RedisWriter
}

func NewStandardRedisWriter(queueName string) *StdOutFileAndRedisWriter {
	return &StdOutFileAndRedisWriter{stdOutAndFileWriter: NewStandardWriter(), redisWriter: NewRedisWriter(queueName)}
}

func NewStdOutFileAndRedisWriter(queueName string, filepath string) (*StdOutFileAndRedisWriter, error) {
	writer := &StdOutFileAndRedisWriter{}
	var err error
	writer.stdOutAndFileWriter, err = NewStdOutAndFileWriter(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to create tdOutFileAndRedisWriter: %s", err.Error())
	}

	writer.redisWriter = NewRedisWriter(queueName)

	return writer, nil
}

func (writer StdOutFileAndRedisWriter) Write(p []byte) (int, error) {
	numBytes, err := writer.stdOutAndFileWriter.Write(p)
	if err != nil {
		return numBytes, fmt.Errorf("failed to write to stdout or file: %s", err.Error())
	}

	numBytes, err = writer.redisWriter.Write(p)
	if err != nil {
		return numBytes, fmt.Errorf("failed to write to redis: %s", err.Error())
	}

	return numBytes, err
}

type StdOutAndFileWriter struct {
	file *os.File
}

func NewStdOutAndFileWriter(filepath string) (*StdOutAndFileWriter, error) {
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create StdOutAndFileWriter because opening of file failed: %s", err.Error())
	}

	return &StdOutAndFileWriter{file: file}, nil
}

func NewStandardWriter() *StdOutAndFileWriter {
	return &StdOutAndFileWriter{file: g_logFile}
}

func (writer StdOutAndFileWriter) Write(p []byte) (int, error) {
	numBytes, err := os.Stdout.Write(p)
	if err != nil {
		return numBytes, fmt.Errorf("failed to write to stdout: %s", err.Error())
	}

	numBytes, err = writer.file.Write(p)
	if err != nil {
		return numBytes, fmt.Errorf("failed to write to file: %s", err.Error())
	}

	return numBytes, err
}

type RedisWriter struct {
	redisQueue *queues.RedisQueue
}

func NewRedisWriter(queueName string) *RedisWriter {
	return &RedisWriter{redisQueue: &queues.RedisQueue{Queue: queueName, Client: redisclient.RedisClient()}}
}

func (writer RedisWriter) Write(p []byte) (int, error) {
	err := writer.redisQueue.Push(string(p))
	if err != nil {
		return 0, fmt.Errorf("failed to write to stdout: %s", err.Error())
	}

	return len(p), nil
}
