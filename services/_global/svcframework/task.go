package svcframework

import (
	"encoding/json"
	"fmt"
	"logger"
	"math"
	"strconv"
	"svcframework/dictionaries"
	"time"

	"github.com/google/uuid"
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

// TODO: Write an umarshall class

func (ts TaskStatus) MarshalJSON() ([]byte, error) {
	// It is assumed Suit implements fmt.Stringer.
	return json.Marshal(ts.String())
}

type TaskRun struct {
	RunNum    uint32        `json:"RunNum"`
	StartTime time.Time     `json:"StartTime"`
	EndTime   time.Time     `json:"EndTime"`
	Runtime   time.Duration `json:"Runtime"`
	Status    TaskStatus    `json:"Status"`
}

func NewTaskRun(runNum uint32) *TaskRun {
	return &TaskRun{runNum, time.Time{}, time.Time{}, 0.0, NOT_RUN}
}

type RunContext struct {
	RedisLogger logger.RedisLogger
}

func NewRunContext(redisLoggerQueue string) *RunContext {
	return &RunContext{RedisLogger: logger.NewRedisLogger(redisLoggerQueue)}
}

type Task struct {
	ID        uuid.UUID
	Payload   string
	NumOfRuns uint32
	Runs      []*TaskRun
}

func NewTask(payload string) *Task {
	return &Task{
		uuid.New(),
		payload,
		0,
		nil,
	}
}

func NewTaskFromDict(dict dictionaries.Dictionary) (*Task, error) {
	IDStr, err := dict.Get("ID")
	if err != nil {
		return nil, fmt.Errorf("failed to create Task object: No [ID] key found in dictionary [%s]", dict.Identifier())
	}
	ID, err := uuid.Parse(IDStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create Task object: ID [%s] in dictionary [%s] could not be parsed as a UUID", IDStr, dict.Identifier())
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

	return &Task{
		ID,
		payload,
		runNum,
		runs,
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

	dict["ID"] = task.ID.String()
	dict["Payload"] = task.Payload
	dict["NumOfRuns"] = strconv.FormatUint(uint64(task.NumOfRuns), 10)
	dict["Runs"] = task.runsToString()

	return dict
}

func (task *Task) GetTaskWithoutPayloadDict() map[string]string {
	dict := task.GetTaskDict()
	delete(dict, "Payload")
	return dict
}

func (task *Task) GetTaskIDAndPayloadDict() map[string]string {
	dict := make(map[string]string)

	dict["ID"] = task.ID.String()
	dict["Payload"] = task.Payload

	return dict
}

func (task *Task) GetErrorDict(errStr string) map[string]string {
	dict := make(map[string]string)

	dict["ID"] = task.ID.String()
	dict["Error"] = errStr

	return dict
}

func (task *Task) IDString() string {
	return task.ID.String()
}

func (task *Task) runsToString() string {
	jsonBlob, _ := json.Marshal(task.Runs)
	return string(jsonBlob)
}

type DelayedTask struct {
	Task      *Task
	RerunTime time.Time
}
