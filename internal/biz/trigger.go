package biz

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BitofferHub/pkg/middlewares/log"
	"github.com/BitofferHub/xtimer/internal/conf"
	"github.com/BitofferHub/xtimer/internal/constant"
	"github.com/BitofferHub/xtimer/internal/utils"
)

// xtimerUseCase is a User usecase.
type TriggerUseCase struct {
	confData  *conf.Data
	timerRepo TimerRepo
	taskRepo  TimerTaskRepo
	taskCache TaskCache
	tm        Transaction
	pool      WorkerPool
	executor  *ExecutorUseCase
}

// NewUserUseCase new a User usecase.
func NewTriggerUseCase(confData *conf.Data, timerRepo TimerRepo, taskRepo TimerTaskRepo, taskCache TaskCache, executorUseCase *ExecutorUseCase) *TriggerUseCase {
	return &TriggerUseCase{
		confData:  confData,
		timerRepo: timerRepo,
		taskRepo:  taskRepo,
		taskCache: taskCache,
		pool:      NewGoWorkerPool(int(confData.Trigger.WorkersNum)),
		executor:  executorUseCase,
	}
}

func (w *TriggerUseCase) Work(ctx context.Context, minuteBucketKey string, ack func()) error {

	// 进行为时一分钟的 zrange 处理
	startTime, err := getStartMinute(minuteBucketKey)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(time.Duration(w.confData.Trigger.ZrangeGapSeconds) * time.Second)
	defer ticker.Stop()

	notifier := NewSafeChan(int(time.Minute / (time.Duration(w.confData.Trigger.ZrangeGapSeconds) * time.Second)))
	defer notifier.Close()

	endTime := startTime.Add(time.Minute)

	// 尝试更新本地缓存
	localCache, _ := w.taskCache.UpdateLocalCache(ctx, minuteBucketKey)
	var wg sync.WaitGroup
	for range ticker.C {
		select {
		case e := <-notifier.GetChan():
			err, _ = e.(error)
			return err
		default:
		}

		wg.Add(1)
		go func(startTime time.Time) {
			defer wg.Done()
			if err := w.handleBatch(ctx, minuteBucketKey, startTime, startTime.Add(time.Duration(w.confData.Trigger.ZrangeGapSeconds)*time.Second), localCache); err != nil {
				notifier.Put(err)
			}
		}(startTime)

		if startTime = startTime.Add(time.Duration(w.confData.Trigger.ZrangeGapSeconds) * time.Second); startTime.Equal(endTime) || startTime.After(endTime) {
			break
		}
	}

	wg.Wait()
	select {
	case e := <-notifier.GetChan():
		err, _ = e.(error)
		return err
	default:
	}
	ack()
	log.InfoContextf(ctx, "ack success, key: %s", minuteBucketKey)
	return nil
}

func (w *TriggerUseCase) handleBatch(ctx context.Context, key string, start, end time.Time, localCache []*TimerTask) error {
	bucket, err := getBucket(key)
	if err != nil {
		return err
	}

	tasks, err := w.getTasksByTime(ctx, key, bucket, start, end, localCache)
	if err != nil || len(tasks) == 0 {
		return err
	}

	timerIDs := make([]int64, 0, len(tasks))
	for _, task := range tasks {
		timerIDs = append(timerIDs, task.TimerID)
	}

	for _, task := range tasks {
		task := task
		if err := w.pool.Submit(func() {
			if err := w.executor.Work(ctx, utils.UnionTimerIDUnix(uint(task.TimerID), task.RunTimer)); err != nil {
				log.ErrorContextf(ctx, "executor work failed, err: %v", err)
			}
		}); err != nil {
			return err
		}
	}
	return nil
}

func (w *TriggerUseCase) getTasksByTime(ctx context.Context, key string, bucket int, start, end time.Time, localCache []*TimerTask) ([]*TimerTask, error) {
	//先查本地缓存
	if len(localCache) != 0 {
		tasks, err := GetTasksByTimeFromLocalCache(ctx, localCache, start.UnixMilli(), end.UnixMilli())
		if err == nil {
			return tasks, nil
		}
	}

	// 第一次先走缓存，更新本地缓存
	tasks, err := w.taskCache.GetTasksByTime(ctx, key, start.UnixMilli(), end.UnixMilli())
	if err == nil {
		return tasks, nil
	}

	// 倘若缓存查询报错，再走db
	tasks, err = w.taskRepo.GetTasksByTimeRange(ctx, start.UnixMilli(), end.UnixMilli(), constant.NotRunned.ToInt())
	if err != nil {
		return nil, err
	}

	maxBucket := w.confData.Scheduler.BucketsNum
	var validTask []*TimerTask
	for _, task := range tasks {
		if uint(task.TimerID)%uint(maxBucket) != uint(bucket) {
			continue
		}
		validTask = append(validTask, task)
	}

	return validTask, nil
}

// 二分查找优化版本
func GetTasksByTimeFromLocalCache(ctx context.Context, localCache []*TimerTask, start, end int64) ([]*TimerTask, error) {
	var tasks []*TimerTask

	// 使用二分查找找到起始位置
	startIdx := binarySearchStart(localCache, start)
	if startIdx == -1 {
		return tasks, nil
	}

	// 从起始位置开始遍历，直到超出end范围
	for i := startIdx; i < len(localCache); i++ {
		if localCache[i].RunTimer >= end {
			break
		}

		tasks = append(tasks, localCache[i])
	}

	return tasks, nil
}

// 二分查找起始位置
func binarySearchStart(localCache []*TimerTask, start int64) int {
	left, right := 0, len(localCache)-1
	result := -1

	for left <= right {
		mid := (left + right) / 2
		unix := localCache[mid].RunTimer

		if unix >= start {
			result = mid
			right = mid - 1
		} else {
			left = mid + 1
		}
	}

	return result
}

func getStartMinute(slice string) (time.Time, error) {
	timeBucket := strings.Split(slice, "_")
	if len(timeBucket) != 2 {
		return time.Time{}, fmt.Errorf("invalid format of msg key: %s", slice)
	}

	return utils.GetStartMinute(timeBucket[0])
}

func getBucket(slice string) (int, error) {
	timeBucket := strings.Split(slice, "_")
	if len(timeBucket) != 2 {
		return -1, fmt.Errorf("invalid format of msg key: %s", slice)
	}
	return strconv.Atoi(timeBucket[1])
}
