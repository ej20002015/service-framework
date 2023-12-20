package svcframework

import (
	"config"
	"fmt"
	"os"
	"os/signal"
	"svcframework/queues"
	"sync"
	"syscall"
)

type Service interface {
	GetTask() string

	OnStartup() error
	OnShutdown() error

	Execute(payload string) error
}

const NUM_WORKER_ROUTINES int = 4

var g_queueTodo queues.Queue
var g_queueErrored queues.Queue
var g_service Service

func Run(service Service) {
	g_service = service
	queuePrefix := fmt.Sprintf("%s:%s:", config.GetConfig().AppName, service.GetTask())

	g_queueTodo = queues.NewRedisQueue(queuePrefix + "TODO")
	g_queueErrored = queues.NewRedisQueue(queuePrefix + "ERRORED")

	err := g_service.OnStartup()
	if err != nil {
		fmt.Printf("Failed so start the service: %s\n", err.Error())
		panic(err)
	}

	workChan := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < NUM_WORKER_ROUTINES; i++ {
		go worker(i, &wg, workChan)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go processSignals(signals, &wg)

	err = g_queueTodo.Push("2, 2")
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
}

func worker(id int, wg *sync.WaitGroup, channel chan string) {
	for {
		val := <-channel
		wg.Add(1)

		fmt.Printf("Worker %d executing job [%s]\n", id, val)
		err := g_service.Execute(val)
		if err != nil {
			g_queueErrored.Push(val)
			fmt.Printf("Worker %d added job [%s] to error queue\n", id, val)
		}

		wg.Done()
	}
}

func processSignals(signalChan chan os.Signal, wg *sync.WaitGroup) {
	for {
		signal := <-signalChan
		exit := false

		fmt.Println()
		switch signal {
		case syscall.SIGINT:
			fmt.Println("SIGINT")
			exit = true
		case syscall.SIGTERM:
			fmt.Println("SIGTERM")
			exit = true

		}

		if exit {
			fmt.Println("Cleaning up and exiting")
			fmt.Println("Waiting for worker threads to finish...")
			wg.Wait()
			fmt.Println("Worker threads finished")
			cleanUp()
		}
	}
}

func cleanUp() {
	err := g_service.OnShutdown()
	if err != nil {
		fmt.Printf("Failed so stop the service: %s\n", err.Error())
		panic(err)
	}

	os.Exit(0)
}
