package queues

import "time"

type Queue interface {
	Push(val string) error
	Pop() (string, error)
	BlockingPop(timeout time.Duration) (string, error)
	Peek() (string, error)
}

const DEFAULT_TIMEOUT time.Duration = time.Second
