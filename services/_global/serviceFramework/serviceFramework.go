package main

import (
	"fmt"
	"serviceFramework/queues"
)

func main() {
	var queue queues.Queue = queues.NewRedisQueue("EVAN_QUEUE")

	var err error
	err = queue.Push("Hello World")
	if err != nil {
		panic(err)
	}

	val, err := queue.Pop()
	if err != nil {
		panic(err)
	}

	err = queue.Push("Hello World 2")
	if err != nil {
		panic(err)
	}

	fmt.Printf("Value in list [%s]\n", val)
}
