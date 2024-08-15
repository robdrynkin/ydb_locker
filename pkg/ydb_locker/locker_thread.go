package ydb_locker

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

func LockerThread(ctx context.Context, deadlineNano *atomic.Int64, db *ydb.Driver, lockName string, ownerName string, ttl time.Duration, reqBuilder LockRequestBuilder) {
	created, err := CreateLock(ctx, db.Table(), lockName, reqBuilder)
	if err != nil {
		log.Fatal("create lock error", err)
		return
	}
	if created {
		log.Printf("lock %s created", lockName)
	}

	for ctx.Err() == nil {
		curOwner, curTimeout, err := TryLock(ctx, db.Table(), lockName, ownerName, ttl, reqBuilder)
		if err == nil && curOwner == ownerName {
			deadlineNano.Store(curTimeout.UnixNano())
		}
		if err != nil {
			log.Println(err)
		}
		time.Sleep(ttl/10 + time.Duration(rand.Int63n(int64(ttl/10))))
	}
}

func RunInLockerThread(ctx context.Context, db *ydb.Driver, lockName string, ownerName string, ttl time.Duration, reqBuilder LockRequestBuilder, f func(deadline time.Time)) {
	var masterDeadline atomic.Int64
	masterDeadline.Store(0)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		LockerThread(ctx, &masterDeadline, db, lockName, ownerName, ttl, reqBuilder)
	}()

	for ctx.Err() == nil {
		deadline := time.Unix(0, masterDeadline.Load())
		if deadline.Compare(time.Now()) > 0 {
			f(deadline)
		}
	}

	wg.Wait()
}
