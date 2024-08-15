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

func LockerThread(ctx context.Context, deadlineNano *atomic.Int64, db *ydb.Driver, lockName string, ownerName string, ttl time.Duration, reqBuilder LockRequestBuilder, events chan struct{}) {
	created, err := CreateLock(ctx, db.Table(), lockName, reqBuilder)
	if err != nil {
		log.Fatal("create lock error", err)
		return
	}
	if created {
		log.Printf("lock %s created", lockName)
	}

	isLockAcquired := false

	for ctx.Err() == nil {
		curOwner, curTimeout, err := TryLock(ctx, db.Table(), lockName, ownerName, ttl, reqBuilder)
		if err == nil && curOwner == ownerName {
			deadlineNano.Store(curTimeout.UnixNano())
			if !isLockAcquired {
				events <- struct{}{}
				isLockAcquired = true
			}
		} else {
			isLockAcquired = false
		}
		if err != nil {
			log.Println(err)
		}
		time.Sleep(ttl/10 + time.Duration(rand.Int63n(int64(ttl/10))))
	}
}

func lockerContext(ctx context.Context, db *ydb.Driver, lockName string, ownerName string, ttl time.Duration, reqBuilder LockRequestBuilder, lockCtxs chan context.Context) {
	var masterDeadline atomic.Int64
	masterDeadline.Store(0)
	var wg sync.WaitGroup
	defer wg.Wait()
	lockAcquiringEvents := make(chan struct{}, 100)

	wg.Add(1)
	go func() {
		defer wg.Done()
		LockerThread(ctx, &masterDeadline, db, lockName, ownerName, ttl, reqBuilder, lockAcquiringEvents)
	}()

	nextProbExpireChan := make(<-chan time.Time)
	var cancel context.CancelFunc
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()

	for {
		select {
		case <-nextProbExpireChan:
			deadline := time.Unix(0, masterDeadline.Load())
			if deadline.Compare(time.Now()) <= 0 {
				cancel()
			} else {
				nextProbExpireChan = time.After(deadline.Sub(time.Now()))
			}

		case <-lockAcquiringEvents:
			deadline := time.Unix(0, masterDeadline.Load())
			nextProbExpireChan = time.After(deadline.Sub(time.Now()))
			var lockCtx context.Context
			lockCtx, cancel = context.WithCancel(ctx)
			lockCtxs <- lockCtx

		case <-ctx.Done():
			return
		}
	}
}

func LockerContext(ctx context.Context, db *ydb.Driver, lockName string, ownerName string, ttl time.Duration, reqBuilder LockRequestBuilder) chan context.Context {
	lockCtxs := make(chan context.Context, 100)

	go func() {
		defer close(lockCtxs)
		lockerContext(ctx, db, lockName, ownerName, ttl, reqBuilder, lockCtxs)
	}()

	return lockCtxs
}
