package gokiq

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/stvp/tempredis"
)

var server, err = tempredis.Start(tempredis.Config{})
var pool = redis.NewPool(func() (redis.Conn, error) {
	c, err := redis.Dial("unix", server.Socket())
	if err != nil {
		return nil, err
	}
	return c, err
}, 3)

func resetRedis(conn redis.Conn) {
	conn.Do("FLUSHDB")
	conn.Close()
}

var args []interface{} = make([]interface{}, 0)
var job = NewJob("HardWorder", "default", args, 0)

func TestEnqueue(t *testing.T) {
	conn := pool.Get()
	defer resetRedis(conn)

	job.Enqueue(pool)

	expected := fmt.Sprintf(`{"jid":"%s","retry":0,"queue":"default","class":"HardWorder","args":[],"enqueued_at":%d}`,
		job.JID,
		job.EnqueuedAt)
	actual, _ := json.Marshal(job)

	if expected != string(actual) {
		t.Errorf("Excepted JSON to be %s, got %s", expected, actual)
	}

	count, _ := redis.Int(conn.Do("SISMEMBER", "queues", job.Queue))

	if count != 1 {
		t.Error("Expected queues list to have the correct queue but didn't found it.")
	}

	count, _ = redis.Int(conn.Do("LLEN", "queue:default"))

	if count != 1 {
		t.Errorf("Expected the queue to have exactly one job but found %d", count)
	}
}

func TestEnqueueAt(t *testing.T) {
	conn := pool.Get()
	defer resetRedis(conn)

	now := time.Now()
	job.EnqueueAt(now, pool)

	b, _ := json.Marshal(job)
	score, _ := redis.Int64(conn.Do("ZSCORE", "schedule", b))

	if score != now.Unix() {
		t.Errorf("Expected the timestamp to be %d but got %d", now.Unix(), score)
	}
}

func TestEnqueueIn(t *testing.T) {
	conn := pool.Get()
	defer resetRedis(conn)

	now := time.Now()
	duration := time.Hour
	job.EnqueueIn(duration, pool)

	b, _ := json.Marshal(job)
	score, _ := redis.Int64(conn.Do("ZSCORE", "schedule", b))
	after := now.Add(duration).Unix()

	if score != after {
		t.Errorf("Expected the timestamp to be %d but got %d", after, score)
	}
}

func TestMultipleEnqueue(t *testing.T) {
	conn := pool.Get()
	defer resetRedis(conn)

	job.Enqueue(pool)
	if err := job.Enqueue(pool); err != nil {
		t.Errorf("Expected enqueue to succeed but got %s", err)
	}
}

func TestMultipleEnqueueAt(t *testing.T) {
	conn := pool.Get()
	defer resetRedis(conn)

	job.EnqueueAt(time.Now(), pool)
	if err := job.EnqueueAt(time.Now(), pool); err != nil {
		t.Errorf("Expected enqueue to succeed but got %s", err)
	}
}

func TestMultipleEnqueueIn(t *testing.T) {
	conn := pool.Get()
	defer resetRedis(conn)

	job.EnqueueIn(5*time.Second, pool)
	if err := job.EnqueueIn(5*time.Second, pool); err != nil {
		t.Errorf("Expected enqueue to succeed but got %s", err)
	}
}

func TestEnqueueReturnsMarshalError(t *testing.T) {
	conn := pool.Get()
	defer resetRedis(conn)

	job := NewJob("foo", "bar", []interface{}{map[int]string{
		0: "Do you pronounce JSON as jay-sun or jay-さん?",
	}}, 0)

	err := job.Enqueue(pool)
	if _, ok := err.(*json.UnsupportedTypeError); !ok {
		t.Errorf("unexpected error: got %T, wanted *json.UnsupportedTypeError", err)
	}
}

func TestEnqueueAtReturnsMarshalError(t *testing.T) {
	conn := pool.Get()
	defer resetRedis(conn)

	job := NewJob("foo", "bar", []interface{}{math.NaN()}, 0)

	err := job.EnqueueAt(time.Now(), pool)
	if _, ok := err.(*json.UnsupportedValueError); !ok {
		t.Errorf("unexpected error: got %T, wanted *json.UnsupportedValueError", err)
	}
}

func TestEnqueueInReturnsMarshalError(t *testing.T) {
	conn := pool.Get()
	defer resetRedis(conn)

	job := NewJob("foo", "bar", []interface{}{map[string]interface{}{
		"rpc magic": func(a, b int) int { return a + b },
	}}, 0)

	err := job.EnqueueAt(time.Now(), pool)
	if _, ok := err.(*json.UnsupportedTypeError); !ok {
		t.Errorf("unexpected error: got %T, wanted *json.UnsupportedTypeError", err)
	}
}
