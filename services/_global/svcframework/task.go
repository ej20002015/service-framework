package svcframework

import (
	"encoding/json"
	"fmt"
	"logger"
	"math"
	"strconv"
	"svcframework/dictionaries"
	"time"
)

type TaskStatus uint32

const (
	SUCCESS TaskStatus = iota
	RUNNING
	ERRORED
	NOT_RUN
)

func (taskStatus TaskStatus) String() string {
	switch taskStatus {
	case SUCCESS:
		return "SUCCESS"
	case RUNNING:
		return "RUNNING"
	case ERRORED:
		return "ERRORED"
	case NOT_RUN:
		return "NOT_RUN"
	}
	return "Unknown TaskStatus"
}

var MapStringToStatus = func() map[string]TaskStatus {
	m := make(map[string]TaskStatus)
	for i := SUCCESS; i <= NOT_RUN; i++ {
		m[i.String()] = i
	}
	return m
}()

func String2TaskStatus(statusStr string) (TaskStatus, error) {
	status, found := MapStringToStatus[statusStr]
	if !found {
		return NOT_RUN, fmt.Errorf("Cannot map [%s] to TaskStatus", statusStr)
	}
	return status, nil
}

// TODO: Write an umarshall class

func (ts TaskStatus) MarshalJSON() ([]byte, error) {
	return json.Marshal(ts.String())
}

type TaskRun struct {
	RunNum    uint32        `json:"RunNum"`
	StartTime time.Time     `json:"StartTime"`
	EndTime   time.Time     `json:"EndTime"`
	Runtime   time.Duration `json:"Runtime"`
	Status    TaskStatus    `json:"Status"`
	Result    string        `json:"Result"`
}

func NewTaskRun(runNum uint32) *TaskRun {
	return &TaskRun{runNum, time.Time{}, time.Time{}, 0.0, NOT_RUN, ""}
}

type RunContext struct {
	RedisLogger logger.RedisLogger
}

func NewRunContext(redisLoggerQueue string) *RunContext {
	return &RunContext{RedisLogger: logger.NewRedisLogger(redisLoggerQueue)}
}

type Task struct {
	ID        string
	Payload   string
	NumOfRuns uint32
	Runs      []*TaskRun
	Result    string
	Status    TaskStatus
}

func NewTask(job *Job) *Task {
	return &Task{
		job.ID,
		job.Payload,
		0,
		nil,
		"",
		NOT_RUN,
	}
}

func NewTaskFromDict(dict dictionaries.Dictionary) (*Task, error) {
	ID, err := dict.Get("ID")
	if err != nil {
		return nil, fmt.Errorf("failed to create Task object: No [ID] key found in dictionary [%s]", dict.Identifier())
	}

	payload, err := dict.Get("Payload")
	if err != nil {
		return nil, fmt.Errorf("failed to create Task object: No [Payload] key found in dictionary [%s]", dict.Identifier())
	}

	runNumStr, err := dict.Get("RunNum")
	if err != nil {
		return nil, fmt.Errorf("failed to create Task object: No [RunNum] key found in dictionary [%s]", dict.Identifier())
	}
	runNumSigned, err := strconv.Atoi(runNumStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create Task object: RunNum [%s] in dictionary [%s] could not be parsed as an integer", runNumStr, dict.Identifier())
	}
	runNum := uint32(runNumSigned)

	runsStr, err := dict.Get("Runs")
	if err != nil {
		return nil, fmt.Errorf("failed to create Task object: No [Runs] key found in dictionary [%s]", dict.Identifier())
	}
	var runs []*TaskRun
	if err := json.Unmarshal([]byte(runsStr), &runs); err != nil {
		return nil, fmt.Errorf("failed to create Task object: Runs [%s] in dictionary [%s] could not be parsed as a list of TaskRuns", runsStr, dict.Identifier())
	}

	result, err := dict.Get("Result")
	if err != nil {
		return nil, fmt.Errorf("failed to create Task object: No [Result] key found in dictionary [%s]", dict.Identifier())
	}

	statusStr, err := dict.Get("Status")
	if err != nil {
		return nil, fmt.Errorf("failed to create Task object: No [Status] key found in dictionary [%s]", dict.Identifier())
	}
	status, err := String2TaskStatus(statusStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse status string as TaskStatus enum: %w", err)
	}

	return &Task{
		ID,
		payload,
		runNum,
		runs,
		result,
		status,
	}, nil
}

func (task *Task) NewRun() *TaskRun {
	task.NumOfRuns++
	task.Runs = append(task.Runs, NewTaskRun(task.NumOfRuns))
	return task.Runs[len(task.Runs)-1]
}

func (task *Task) NewRunContext(redisRoot string) *RunContext {
	redisQueueName := fmt.Sprintf("%s:RUN_%d:LOG", redisRoot, task.NumOfRuns)
	return NewRunContext(redisQueueName)
}

func (task *Task) CalcDelayTime(delay time.Duration, delayFactor float64) time.Time {
	lastRunTime := task.Runs[len(task.Runs)-1].EndTime
	if task.NumOfRuns < 1 {
		return lastRunTime
	}

	delayTime := float64(delay) * math.Pow(delayFactor, float64(task.NumOfRuns-1))
	return lastRunTime.Add(time.Duration(delayTime))
}

func (task *Task) GetTaskDict() map[string]string {
	dict := make(map[string]string)

	dict["ID"] = task.ID
	dict["Payload"] = task.Payload
	dict["NumOfRuns"] = strconv.FormatUint(uint64(task.NumOfRuns), 10)
	dict["Runs"] = task.runsToString()
	dict["Result"] = task.Result
	dict["Status"] = task.Status.String()

	return dict
}

func (task *Task) GetTaskWithoutPayloadDict() map[string]string {
	dict := task.GetTaskDict()
	delete(dict, "Payload")
	return dict
}

func (task *Task) GetTaskIDAndPayloadDict() map[string]string {
	dict := make(map[string]string)

	dict["ID"] = task.ID
	dict["Payload"] = task.Payload

	return dict
}

func (task *Task) FinaliseRun() {
	if len(task.Runs) == 0 {
		return
	}

	lastRun := task.Runs[len(task.Runs)-1]
	task.Result = lastRun.Result
	task.Status = lastRun.Status
}

func (task *Task) runsToString() string {
	jsonBlob, _ := json.Marshal(task.Runs)
	return string(jsonBlob)
}

type DelayedTask struct {
	Task      *Task
	RerunTime time.Time
}

type Job struct {
	ID      string `json:"ID"`
	Payload string `json:"Payload"`
}

func NewJobFromString(jobStr string) (*Job, error) {
	var job *Job
	if err := json.Unmarshal([]byte(jobStr), &job); err != nil {
		return nil, fmt.Errorf("failed to create Job object: jobStr could not be parsed as a Job object")
	}

	return job, nil
}
