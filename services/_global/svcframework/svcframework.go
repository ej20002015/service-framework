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

	"github.com/spf13/viper"
)

type Service interface {
	GetTask() string

	OnStartup() error
	OnShutdown() error

	Execute(payload string, runCtx *RunContext) (TaskStatus, string, error)
}

var g_queueTodo queues.Queue
var g_queueDone queues.Queue
var g_queueErrored queues.Queue
var g_service Service
var g_queuePrefix string
var g_taskDictPrefix string

func Logger() *logger.StdLogger {
	return &logger.Logger
}

func Run(service Service) {
	config.Init()
	logger.Init()

	g_service = service
	g_queuePrefix = fmt.Sprintf("%s:%s:", viper.GetString("AppName"), service.GetTask())
	g_taskDictPrefix = g_queuePrefix + "TASK_DICTS:"

	g_queueTodo = queues.NewRedisQueue(g_queuePrefix + "TODO")
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
	for i := 0; i < viper.GetInt("NumWorkerThreads"); i++ {
		go worker(i, &wg, workerChan, delayedTaskChan)
	}

	go processDelayedTasks(workerChan, delayedTaskChan)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go processSignals(signals, &wg)

	for {
		jobStr, err := g_queueTodo.BlockingPop(queues.INFINITE_TIMEOUT)
		if err != nil {
			Logger().Fatal().Msg(fmt.Sprintf("Failed to pop job of TODO queue: %s", err.Error()))
			return
		}
		job, err := NewJobFromString(jobStr)
		if err != nil {
			Logger().Error().Msg(fmt.Sprintf("Couldn't parse jobStr [%s] as a Job object", jobStr))
			continue
		}

		task := NewTask(job)
		Logger().Info().Msg(fmt.Sprintf("Task [%s]: created with payload [%s]", task.ID, task.Payload))
		dictName := g_taskDictPrefix + task.ID + ":INFO"
		dictionaries.NewRedisDictionaryFromMap(dictName, task.GetTaskDict())

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
		taskRedisRoute := g_taskDictPrefix + task.ID
		runContext := task.NewRunContext(taskRedisRoute)

		runContext.RedisLogger.Info().Msg(fmt.Sprintf("%s: being executed by Worker %d...", runStr, id))

		run.StartTime = time.Now()
		taskStatus, result, err := g_service.Execute(task.Payload, runContext)
		run.EndTime = time.Now()
		run.Runtime = time.Duration(run.EndTime.Sub(run.StartTime))
		run.Status = taskStatus
		run.Result = result

		if taskStatus != ERRORED {
			g_queueDone.Push(task.ID)
		} else {
			var errMsg string
			if err != nil {
				errMsg = err.Error()
			} else {
				errMsg = "NO ERROR MESSAGE"
			}

			run.Result = errMsg
			runContext.RedisLogger.Error().Msg(fmt.Sprintf("%s: execution errored - %s", runStr, errMsg))

			nextRunNum := task.NumOfRuns + 1
			if nextRunNum <= viper.GetUint32("MaxRuns") {
				nextRuntime := task.CalcDelayTime(viper.GetDuration("Delay"), viper.GetFloat64("DelayFactor"))
				delayedTaskChan <- &DelayedTask{task, nextRuntime}
				runContext.RedisLogger.Info().Msg(fmt.Sprintf("%s: next run [%d] within MaxRun limit [%d] - will be rerun at [%s]", runStr, nextRunNum, viper.GetUint32("MaxRuns"), nextRuntime.Format(time.RFC3339Nano)))
			} else {
				g_queueErrored.Push(task.ID)
				runContext.RedisLogger.Info().Msg(fmt.Sprintf("%s: hit MaxRun limit [%d] - placing in error queue", runStr, viper.GetUint32("MaxRuns")))
			}
		}

		// Save task state
		task.FinaliseRun()
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
