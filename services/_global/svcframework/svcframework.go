package svcframework

import (
	"config"
	"encoding/json"
	"fmt"
	"logger"
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

	Execute(payload string, runCtx *RunContext) (TaskStatus, error)
}

const NUM_WORKER_THREADS int = 4

var g_queueTodo queues.Queue
var g_queueSeen queues.Queue
var g_queueDone queues.Queue
var g_queueErrored queues.Queue
var g_service Service
var g_queuePrefix string
var g_taskDictPrefix string

func Logger() *logger.StdLogger {
	return &logger.Logger
}

func Run(service Service) {
	logger.Init()

	g_service = service
	g_queuePrefix = fmt.Sprintf("%s:%s:", config.GetConfig().App, service.GetTask())
	g_taskDictPrefix = g_queuePrefix + "TASK_DICTS:"

	g_queueTodo = queues.NewRedisQueue(g_queuePrefix + "TODO")
	g_queueSeen = queues.NewRedisQueue(g_queuePrefix + "SEEN")
	g_queueDone = queues.NewRedisQueue(g_queuePrefix + "DONE")
	g_queueErrored = queues.NewRedisQueue(g_queuePrefix + "ERRORED")

	Logger().Info().Msg("Service starting up...")
	err := g_service.OnStartup()
	if err != nil {
		Logger().Fatal().Msg(fmt.Sprintf("Failed to start the service: %s", err.Error()))
		return
	}
	Logger().Info().Msg("Service finished starting up")

	workerChan := make(chan *Task, 100)
	delayedTaskChan := make(chan *DelayedTask, 100)
	var wg sync.WaitGroup
	for i := 0; i < NUM_WORKER_THREADS; i++ {
		go worker(i, &wg, workerChan, delayedTaskChan)
	}

	go processDelayedTasks(workerChan, delayedTaskChan)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go processSignals(signals, &wg)

	for {
		payload, err := g_queueTodo.BlockingPop(queues.INFINITE_TIMEOUT)
		if err != nil {
			Logger().Fatal().Msg(fmt.Sprintf("Failed to pop job of TODO queue: %s", err.Error()))
			return
		}
		if payload == "" {
			continue
		}

		task := NewTask(payload)
		Logger().Info().Msg(fmt.Sprintf("Task [%s]: created with payload [%s]", task.ID, task.Payload))
		err = g_queueSeen.Push(task.IDString())
		if err != nil {
			Logger().Fatal().Msg(fmt.Sprintf("Failed to push job onto SEEN queue: %s", err.Error()))
			return
		}

		// If failed then redo task
		// Have ability to log to redis
		// Log some sort of result

		workerChan <- task

		Logger().Info().Msg(fmt.Sprintf("Task [%s]: placed in work queue", task.ID))
	}
}

func worker(id int, wg *sync.WaitGroup, workChan chan *Task, delayedTaskChan chan *DelayedTask) {
	for {
		task := <-workChan
		wg.Add(1)

		run := task.NewRun()
		runStr := fmt.Sprintf("Task [%s:RUN_%d]", task.ID, run.RunNum)
		taskRedisRoute := g_taskDictPrefix + task.IDString()
		runContext := task.NewRunContext(taskRedisRoute)

		runContext.RedisLogger.Info().Msg(fmt.Sprintf("%s: being executed by Worker %d...", runStr, id))

		run.StartTime = time.Now()
		taskStatus, err := g_service.Execute(task.Payload, runContext)
		run.EndTime = time.Now()
		run.Runtime = time.Duration(run.EndTime.Sub(run.StartTime))
		run.Status = taskStatus

		if taskStatus != ERRORED {
			g_queueDone.Push(task.IDString())
		} else {
			var errMsg string
			if err != nil {
				errMsg = err.Error()
			} else {
				errMsg = "NO ERROR MESSAGE"
			}

			runContext.RedisLogger.Error().Msg(fmt.Sprintf("%s: execution errored - %s", runStr, errMsg))

			nextRunNum := task.NumOfRuns + 1
			if nextRunNum <= config.GetConfig().MaxRuns {
				nextRuntime := task.CalcDelayTime(config.GetConfig().Delay, config.GetConfig().DelayFactor)
				delayedTaskChan <- &DelayedTask{task, nextRuntime}
				runContext.RedisLogger.Info().Msg(fmt.Sprintf("%s: next run [%d] within MaxRun limit [%d] - will be rerun at [%s]", runStr, nextRunNum, config.GetConfig().MaxRuns, nextRuntime.Format(time.RFC3339Nano)))
			} else {
				errDict := task.GetErrorDict(errMsg)
				g_queueErrored.Push(dictToJson(errDict)) // TODO: fix the fact that when you stop the service you lose all info about jobs being rerun
				runContext.RedisLogger.Info().Msg(fmt.Sprintf("%s: hit MaxRun limit [%d] - placing in error queue", runStr, config.GetConfig().MaxRuns))
			}
		}

		// Save task state
		dictName := taskRedisRoute + ":INFO"
		dictionaries.NewRedisDictionaryFromMap(dictName, task.GetTaskDict())

		runContext.RedisLogger.Info().Msg(fmt.Sprintf("%s: finished execution by Worker %d - status: %s", runStr, id, taskStatus.String()))

		wg.Done()
	}
}

func processDelayedTasks(workChan chan *Task, delayedTaskChan chan *DelayedTask) {
	for {
		delayedTask := <-delayedTaskChan
		if time.Now().After(delayedTask.RerunTime) {
			workChan <- delayedTask.Task
		} else {
			delayedTaskChan <- delayedTask
		}
	}
}

func processSignals(signalChan chan os.Signal, wg *sync.WaitGroup) {
	for {
		signal := <-signalChan
		exit := false

		switch signal {
		case syscall.SIGINT:
			Logger().Info().Msg("SIGINT")
			exit = true
		case syscall.SIGTERM:
			Logger().Info().Msg("SIGTERM")
			exit = true

		}

		if exit {
			Logger().Info().Msg("Cleaning up and exiting")
			Logger().Info().Msg("Waiting for worker threads to finish...")
			wg.Wait()
			Logger().Info().Msg("Worker threads finished")
			cleanUp()
		}
	}
}

func cleanUp() {
	err := g_service.OnShutdown()
	if err != nil {
		Logger().Fatal().Msg(fmt.Sprintf("Failed to stop the service: %s", err.Error()))
		return
	}

	logger.Shutdown()
	os.Exit(0)
}

func dictToJson(dict map[string]string) string {
	jsonBlob, _ := json.Marshal(dict)
	return string(jsonBlob)
}
