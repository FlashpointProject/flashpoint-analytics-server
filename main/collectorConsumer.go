package main

import (
	"context"
	"sync"
	"time"

	"github.com/Dri0m/flashpoint-analytics-server/utils"
	"github.com/sirupsen/logrus"
)

func (a *App) RunCollectorConsumer(logger *logrus.Entry, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	l := logger.WithField("serviceName", "collectorConsumer")
	defer l.Info("consumer stopped")

	bucket, ticker := utils.NewBucketLimiter(50*time.Millisecond, 1)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			l.Info("context cancelled, stopping collector consumer once the queue is empty")
			for len(a.collectQueue) > 0 {
				a.IngestCollectQueueBatch(logger, ctx)
			}
			return
		case <-bucket:
			if len(a.collectQueue) > 0 {
				a.IngestCollectQueueBatch(logger, ctx)
			}
		}
	}
}

func (a *App) IngestCollectQueueBatch(logger *logrus.Entry, ctx context.Context) {
	const batchSize = 1000

	rows := [][]interface{}{}

	readUpTo := batchSize
	qlen := len(a.collectQueue)
	if qlen < batchSize {
		readUpTo = qlen
	}
	for i := 0; i < readUpTo; i++ {
		event, ok := <-a.collectQueue
		if !ok {
			break
		}
		rows = append(rows, []interface{}{event.SessionID, event.Category, event.Key, event.Value, event.CreatedAt})
	}

	logger.Debugf("inserting a batch of %d events", readUpTo)

	err := a.DAL.InsertEventBatch(ctx, rows)
	if err != nil {
		a.l.Error(err)
	}
}
