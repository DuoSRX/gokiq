package gokiq

import (
	"github.com/garyburd/redigo/redis"
	"testing"
	"time"
)

func resetRedis(pool *redis.Pool) {
	conn := pool.Get()
	defer conn.Close()
	conn.Do("FLUSHDB")
}

func TestGokiq(t *testing.T) {
	pool := redis.NewPool(func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", ":6379")
		if err != nil {
			return nil, err
		}
		return c, err
	}, 3)

	conn := pool.Get()
	defer conn.Close()
	defer pool.Close()

	job := NewJob("HardWorder", "default", []string{})
	job.Enqueue(pool)

	count, _ := redis.Int(conn.Do("SISMEMBER", "queues", job.Queue))

	if count != 1 {
		t.Error("Expected queues list to have the correct queue but didn't found it.")
	}

	count, _ = redis.Int(conn.Do("LLEN", "queue:default"))

	if count != 1 {
		t.Errorf("Expected the queue to have exactly one job but found %d", count)
	}

	now := time.Now()
	job.EnqueueAt(now, pool)

	score, _ := redis.Int64(conn.Do("ZSCORE", "schedule", job.toJSON()))

	if score != now.Unix() {
		t.Errorf("Expected the timestamp to be %d but go %d", score, now.Unix())
	}

	resetRedis(pool)
}
