package main

import (
	"fmt"
	"strconv"
	"strings"
	"svcframework"
)

type Adder struct{}

func (adder *Adder) GetTask() string {
	return "Add"
}

func (adder *Adder) OnStartup() error  { return nil }
func (adder *Adder) OnShutdown() error { return nil }

func (adder *Adder) Execute(payload string, runCtx *svcframework.RunContext) (svcframework.TaskStatus, string, error) {
	spltStr := strings.Split(payload, ",")
	firstStr := strings.Trim(spltStr[0], " ")
	firstNum, err := strconv.Atoi(firstStr)
	if err != nil {
		return svcframework.ERRORED, "", fmt.Errorf("first number [%s] in task [%s] cannot be converted to a number", firstStr, payload)
	}

	secondStr := strings.Trim(spltStr[1], " ")
	secondNum, err := strconv.Atoi(secondStr)
	if err != nil {
		return svcframework.ERRORED, "", fmt.Errorf("second number [%s] in task [%s] cannot be converted to a number", secondStr, payload)
	}

	result := firstNum + secondNum
	resultStr := fmt.Sprintf("Result of task [%s] is [%d]", payload, result)
	runCtx.RedisLogger.Info().Msg(resultStr)

	return svcframework.SUCCESS, resultStr, nil
}

func main() {
	svcframework.Run(&Adder{})
}
