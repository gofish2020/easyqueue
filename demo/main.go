package main

import (
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofish2020/easyqueue"
)

var g_Queue = easyqueue.CreateEasyQueue(easyqueue.SetQueueParttion(1), easyqueue.SetQueueCapacity(100), easyqueue.SetWorkerNum(1))

var cacheNum atomic.Int64
var errCount atomic.Int64
var sucessCount atomic.Int64
var selloutCount atomic.Int64

var pingCount atomic.Int64

func main() {

	// 20个库存的商品
	cacheNum.Store(20)

	r := gin.Default()

	// 获取调用结果
	r.GET("/cache", func(c *gin.Context) {

		c.JSON(http.StatusOK, gin.H{
			"商品剩余数量":      strconv.Itoa(int(cacheNum.Load())),
			"售罄->没买到的用户":  strconv.Itoa(int(selloutCount.Load())),
			"队列满->丢弃的用户数": strconv.Itoa(int(errCount.Load())),
			"成功->抢购的用户数":  strconv.Itoa(int(sucessCount.Load())),
			"抢购链接->总调用次数": strconv.Itoa(int(pingCount.Load())),
		})
	})

	// 增加商品库存
	r.GET("/add", func(c *gin.Context) {
		cacheNum.Add(20)

		c.JSON(http.StatusOK, gin.H{
			"message": "success",
		})
	})

	r.GET("/ping", func(c *gin.Context) {

		pingCount.Add(1)

		// 如果已经售罄直接退出，不进入队列
		if cacheNum.Load() == 0 {
			selloutCount.Add(1)
			c.JSON(http.StatusOK, gin.H{
				"message": "商品售罄",
			})

			return
		}

		// 进入队列
		waitJob := g_Queue.Push(func() {

			//1. 先判断是否售罄
			if cacheNum.Load() == 0 {
				selloutCount.Add(1)
				c.JSON(http.StatusOK, gin.H{
					"message": "商品售罄",
				})
			} else { //2.未售罄

				// 3.扣减库存
				cacheNum.Add(-1)
				// 4.下单逻辑..(模拟业务有点慢)
				time.Sleep(500 * time.Millisecond)
				// 5.告诉客户抢购成功
				sucessCount.Add(1)
				c.JSON(http.StatusOK, gin.H{
					"message": "抢购成功",
				})
			}

		})

		// 阻塞等待
		<-waitJob.Done()

		if waitJob.Err() == easyqueue.ErrOverFlow { // 表示进入队列失败（队列满了)
			errCount.Add(1)
			c.JSON(http.StatusOK, gin.H{
				"message": "抢购人数过多，请重试...",
			})
		}
	})
	r.Run() // listen and serve on 0.0.0.0:8080 (for windows "localhost:8080")
}
