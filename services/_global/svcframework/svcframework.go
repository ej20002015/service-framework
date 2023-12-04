package svcframework

import (
	"config"
	"fmt"
	"svcframework/queues"
)

type Service interface {
	GetTask() string

	OnStartup() error
	OnShutdown() error

	Execute(payload string) error
}

var g_queueTodo queues.Queue
var g_queueErrored queues.Queue
var g_service Service

func Run(service Service) {
	g_service = service
	queuePrefix := fmt.Sprintf("%s:%s:", config.GetConfig().AppName, service.GetTask())

	g_queueTodo = queues.NewRedisQueue(queuePrefix + "TODO")
	g_queueErrored = queues.NewRedisQueue(queuePrefix + "ERRORED")

	service.OnStartup()

	workChan := make(chan string)
	for i := 0; i < 4; i++ {
		go worker(i, workChan)
	}

	err := g_queueTodo.Push("2, 2")
	if err != nil {
		panic(err)
	}

	for {
		val, err := g_queueTodo.BlockingPop(queues.INFINITE_TIMEOUT)
		if err != nil {
			fmt.Println(err.Error())
			panic(err.Error())
		}

		if val == "" {
			continue
		}

		fmt.Printf("Value in list [%s]\n", val)

		workChan <- val

		fmt.Println("Finished putting in queue")
	}

	// TODO: Gracefully handle shutdown of service
	//service.OnShutdown()
}

func worker(id int, channel chan string) {
	for {
		val := <-channel
		fmt.Printf("Worker %d executing job [%s]\n", id, val)
		err := g_service.Execute(val)
		if err != nil {
			g_queueErrored.Push(val)
			fmt.Printf("Worker %d added job [%s] to error queue\n", id, val)
		}
	}
}
