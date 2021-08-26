package gokiq

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Job holds all the information about the job to be enqueued.
type Job struct {
	JID        string        `json:"jid"`
	Retry      int           `json:"retry"`
	Queue      string        `json:"queue"`
	Class      string        `json:"class"`
	Args       []interface{} `json:"args"`
	EnqueuedAt int64         `json:"enqueued_at"`
}

// randomHex generates a random string in hexadecimal form of length n * 2.
func randomHex(n int) string {
	id := make([]byte, n)
	io.ReadFull(rand.Reader, id)
	return hex.EncodeToString(id)
}

// Enqueue inserts the job into the Sidekiq queue instantly.
func (job *Job) Enqueue(pool *redis.Pool) error {
	conn := pool.Get()
	defer conn.Close()

	b, err := json.Marshal(job)
	if err != nil {
		return err
	}
	_, err = conn.Do("SADD", "queues", job.Queue)
	if err != nil {
		return err
	}
	_, err = conn.Do("LPUSH", "queue:"+job.Queue, b)
	return err
}

// EnqueueAt insert the job into the Sidekiq scheduled queue at the given time.
func (job *Job) EnqueueAt(time time.Time, pool *redis.Pool) error {
	conn := pool.Get()
	defer conn.Close()

	b, err := json.Marshal(job)
	if err != nil {
		return err
	}
	_, err = conn.Do("ZADD", "schedule", time.Unix(), b)
	return err
}

// EnqueueIn insert the job into the queue after the given duration.
func (job *Job) EnqueueIn(duration time.Duration, pool *redis.Pool) error {
	t := time.Now().Add(duration)
	return job.EnqueueAt(t, pool)
}

// NewJob initialize a new job given a class, queue and arguments.
func NewJob(class, queue string, args []interface{}, retry int) *Job {
	return &Job{
		JID:        randomHex(12),
		Retry:      retry,
		Queue:      queue,
		Class:      class,
		Args:       args,
		EnqueuedAt: time.Now().Unix(),
	}
}
