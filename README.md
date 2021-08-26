# Gokiq [![Build Status](https://travis-ci.org/DuoSRX/gokiq.svg?branch=master)](https://travis-ci.org/DuoSRX/gokiq)

Gokiq is a small library to easily enqueue Sidekiq jobs from Go.

## Usage

``` go
import (
  "github.com/duosrx/gokiq"
  "github.com/gomodule/redigo/redis"
  "time"
)

// Create a Redis Pool first
var pool = &redis.Pool{
	MaxIdle:     3,
	IdleTimeout: 240 * time.Second,
	Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", ":6379")
	},
}

job := NewJob("HardWorker", "default", []string{"foo", "bar"})

// Enqueue immediately...
job.Enqueue(pool)

// ... or enqueue in the future
now := time.Now()
job.EnqueueAt(now, pool)
```

