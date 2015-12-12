package gokiq

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	ErrInsertion = errors.New("Couldn't insert job into queue.")
)

// Job holds all the information about the job to be enqueued.
type Job struct {
	JID        string   `json:"jid"`
	Retry      int      `json:"retry"`
	Queue      string   `json:"queue"`
	Class      string   `json:"class"`
	Args       []string `json:"args"`
	EnqueuedAt int64    `json:"enqueued_at"`
}

// get generate a random string in hexadecimal form of length n * 2.
func randomHex(n int) string {
	id := make([]byte, n)
	io.ReadFull(rand.Reader, id)
	return hex.EncodeToString(id)
}

// toJSON serialize the code into a JSON string
func (job *Job) toJSON() string {
	encoded, _ := json.Marshal(job)
	return string(encoded)
}

// Enqueue insert the job into the Sidekiq queue instantly.
func (job *Job) Enqueue(pool *redis.Pool) error {
	conn := pool.Get()
	defer conn.Close()

	conn.Send("SADD", "queues", job.Queue)
	conn.Send("LPUSH", "queue:"+job.Queue, job.toJSON())
	conn.Flush()

	// Receive the SADD response
	res, err := redis.Int(conn.Receive())
	if err != nil || res == 0 {
		return ErrInsertion
	}

	// Receive the LPUSH response
	res, err = redis.Int(conn.Receive())
	if err != nil || res == 0 {
		return ErrInsertion
	}

	return nil
}

// EnqueueAt insert the job into the Sidekiq scheduled queue at the given time.
func (job *Job) EnqueueAt(time time.Time, pool *redis.Pool) error {
	conn := pool.Get()
	defer conn.Close()

	res, err := redis.Int(conn.Do("ZADD", "schedule", time.Unix(), job.toJSON()))

	if err != nil || res == 0 {
		return ErrInsertion
	}

	return nil
}

// EnqueueIn insert the job into the queue after the given duration
func (job *Job) EnqueueIn(duration time.Duration, pool *redis.Pool) error {
	t := time.Now().Add(duration)
	return job.EnqueueAt(t, pool)
}

// NewJob initialize a new job given a class, queue and arguments.
func NewJob(class, queue string, args []string, retry int) *Job {
	job := &Job{
		JID:        randomHex(12),
		Retry:      retry,
		Queue:      queue,
		Class:      class,
		Args:       args,
		EnqueuedAt: time.Now().Unix(),
	}

	return job
}
