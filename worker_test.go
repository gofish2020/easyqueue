package easyqueue

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreateWorkerMange(t *testing.T) {

	wg := sync.WaitGroup{}
	jobWg := sync.WaitGroup{}

	var count atomic.Int64
	var errCount atomic.Int64
	var allJobNum int64 = 10000 // 1w

	queue := createJobQueue(500)
	createWorkerMange(queue, 5) // 启动5个worker执行队列任务

	for i := 0; i < 3; i++ { //总共来了3波

		for i := int64(0); i < allJobNum; i++ {
			wg.Add(1) // 保证下面的协程启动完成

			jobWg.Add(1) // 默认job可以加入队列

			go func() {
				job := newJob(func() {
					count.Add(1)
					jobWg.Done() // 加入成功
				})
				queue.Push(job)

				if job.Err() != nil {
					errCount.Add(1)
					jobWg.Done() // 加入失败
				}

				wg.Done()
			}()

		}

		time.Sleep(1 * time.Second) // 每秒 1w Qps
	}

	wg.Wait()    // 保证 go 协程都启动
	jobWg.Wait() // 保证job都执行完成

	t.Log("丢弃的任务", errCount.Load())
	t.Log("成功的任务", count.Load())

	assert.Equal(t, int64(errCount.Load()+count.Load()), 3*allJobNum)

}

func TestCreateMultiJobQueue(t *testing.T) {

	jobWg := sync.WaitGroup{}

	var count atomic.Int64
	var errCount atomic.Int64
	var allJobNum int64 = 10000 // 1w

	queue := createMultiJobQueue(5, 100)
	createWorkerMange(queue, 5) // 启动5个worker执行队列任务

	for i := 0; i < 3; i++ { //总共来了3波

		for i := int64(0); i < allJobNum; i++ {

			jobWg.Add(1) // 默认job可以加入队列

			go func() {
				job := newJob(func() {
					count.Add(1)
					jobWg.Done() // 加入成功
				})
				queue.Push(job)

				if job.Err() != nil {
					errCount.Add(1)
					jobWg.Done() // 加入失败
				}

			}()

		}

		time.Sleep(1 * time.Second) // 每秒 1w Qps
	}

	jobWg.Wait() // 保证job都执行完成

	t.Log("丢弃的任务", errCount.Load())
	t.Log("成功的任务", count.Load())

	assert.Equal(t, int64(errCount.Load()+count.Load()), 3*allJobNum)

}
