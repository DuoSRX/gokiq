package gokiq

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"io"
	"time"
)

// Job holds all the information about the job to be enqueued.
type Job struct {
	JID        string   `json:"jid"`
	Retry      bool     `json:"retry"`
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
func (job *Job) Enqueue(pool *redis.Pool) string {
	conn := pool.Get()
	defer conn.Close()

	conn.Send("SADD", "queues", job.Queue)
	conn.Send("LPUSH", "queue:"+job.Queue, job.toJSON())
	conn.Flush()

	return job.JID
}

// Enqueue insert the job into the Sidekiq scheduled queue at the given time.
func (job *Job) EnqueueAt(time time.Time, pool *redis.Pool) string {
	conn := pool.Get()
	defer conn.Close()

	conn.Send("ZADD", "schedule", time.Unix(), job.toJSON())
	conn.Flush()

	return job.JID
}

// NewJob initialize a new job given a class, queue and arguments.
func NewJob(class, queue string, args []string) *Job {
	job := &Job{
		JID:        randomHex(12),
		Retry:      false,
		Queue:      queue,
		Class:      class,
		Args:       args,
		EnqueuedAt: time.Now().Unix(),
	}

	return job
}
