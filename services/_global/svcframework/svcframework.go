package svcframework

import (
	"config"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"svcframework/dictionaries"
	"svcframework/queues"
	"sync"
	"syscall"
	"time"
)

type Service interface {
	GetTask() string

	OnStartup() error
	OnShutdown() error

	Execute(payload string) (TaskStatus, error)
}

const NUM_WORKER_THREADS int = 4

var g_queueTodo queues.Queue
var g_queueSeen queues.Queue
var g_queueDone queues.Queue
var g_queueErrored queues.Queue
var g_service Service
var g_queuePrefix string
var g_taskDictPrefix string

func Run(service Service) {
	g_service = service
	g_queuePrefix = fmt.Sprintf("%s:%s:", config.GetConfig().AppName, service.GetTask())
	g_taskDictPrefix = g_queuePrefix + "TASK_DICTS:"

	g_queueTodo = queues.NewRedisQueue(g_queuePrefix + "TODO")
	g_queueSeen = queues.NewRedisQueue(g_queuePrefix + "SEEN")
	g_queueDone = queues.NewRedisQueue(g_queuePrefix + "DONE")
	g_queueErrored = queues.NewRedisQueue(g_queuePrefix + "ERRORED")

	err := g_service.OnStartup()
	if err != nil {
		fmt.Println("Failed to start the service")
		panic(err)
	}

	workChan := make(chan *Task)
	var wg sync.WaitGroup
	for i := 0; i < NUM_WORKER_THREADS; i++ {
		go worker(i, &wg, workChan)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go processSignals(signals, &wg)

	for {
		payload, err := g_queueTodo.BlockingPop(queues.INFINITE_TIMEOUT)
		if err != nil {
			panic(err)
		}
		if payload == "" {
			continue
		}

		task := NewTask(payload)
		fmt.Printf("Task [%s]: created with payload [%s]\n", task.ID, task.Payload)
		err = g_queueSeen.Push(dictToJson(task.GetTaskIDAndPayloadDict()))
		if err != nil {
			panic(err)
		}

		// If failed then redo task
		// Have ability to log to redis
		// Log some sort of result

		workChan <- task

		fmt.Printf("Task [%s]: placed in work queue\n", task.ID)
	}
}

func worker(id int, wg *sync.WaitGroup, channel chan *Task) {
	for {
		task := <-channel
		wg.Add(1)

		fmt.Printf("Task [%s]: being executed by Worker %d...\n", task.ID, id)

		run := task.NewRun()
		run.StartTime = time.Now()
		taskStatus, err := g_service.Execute(task.Payload) // TODO: Create TaskContext and pass in (should be able to write to output dict from within service)
		run.EndTime = time.Now()
		run.Status = taskStatus

		if err != nil {
			errDict := task.GetErrorDict(err.Error())
			g_queueErrored.Push(dictToJson(errDict)) // TODO: work out how to redo errored tasks correctly
			fmt.Printf("Task [%s]: execution errored - added to error queue\n", task.ID)
		}

		g_queueDone.Push(task.IDString()) // TODO: If errored do not put on the done queue unless we've hit the max retries

		// Save task state
		dictName := g_taskDictPrefix + task.IDString()
		dictionaries.NewRedisDictionaryFromMap(dictName, task.GetTaskWithoutPayloadDict())

		fmt.Printf("Task [%s]: finished execution by Worker %d\n", task.ID, id)

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
		fmt.Println("Failed to stop the service")
		panic(err)
	}

	os.Exit(0)
}

func dictToJson(dict map[string]string) string {
	jsonBlob, _ := json.Marshal(dict)
	return string(jsonBlob)
}
