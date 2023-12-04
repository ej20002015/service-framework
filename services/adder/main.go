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

func (adder *Adder) Execute(payload string) error {
	spltStr := strings.Split(payload, ",")
	firstStr := strings.Trim(spltStr[0], " ")
	firstNum, err := strconv.Atoi(firstStr)
	if err != nil {
		return fmt.Errorf("first number [%s] in task [%s] cannot be converted to a number", payload, firstStr)
	}

	secondStr := strings.Trim(spltStr[1], " ")
	secondNum, err := strconv.Atoi(secondStr)
	if err != nil {
		return fmt.Errorf("second number [%s] in task [%s] cannot be converted to a number", payload, secondStr)
	}

	result := firstNum + secondNum
	fmt.Printf("Result of task [%s] is [%d]\n", payload, result)

	return nil
}

func main() {
	svcframework.Run(&Adder{})
}
