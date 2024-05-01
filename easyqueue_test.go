package easyqueue

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreateEasyQueue(t *testing.T) {
	jobWg := sync.WaitGroup{}

	var count atomic.Int64
	var errCount atomic.Int64
	var allJobNum int64 = 10000 // 1w

	queue := CreateEasyQueue(SetQueueParttion(5), SetQueueCapacity(100), SetWorkerNum(5))

	for i := 0; i < 3; i++ { //总共来了3波

		for i := int64(0); i < allJobNum; i++ { // 每波1w QPS

			jobWg.Add(1)

			go func() {

				// 放入队列
				waitJob := queue.Push(func() {
					count.Add(1)
					jobWg.Done() // 加入成功
				})

				if waitJob.Err() != nil {
					errCount.Add(1)
					jobWg.Done() // 加入失败
				}

				<-waitJob.Done()

			}()

		}

		time.Sleep(1 * time.Second) // 每秒 1w Qps
	}

	jobWg.Wait() // 保证job都执行完成

	t.Log("丢弃的任务", errCount.Load())
	t.Log("成功的任务", count.Load())

	assert.Equal(t, int64(errCount.Load()+count.Load()), 3*allJobNum)
}
