package ydb_locker

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

func LockerThread(ctx context.Context, deadlineNano *atomic.Int64, lockStorage LockStorage, lockName string, ownerName string, ttl time.Duration, events chan struct{}, funcsToRun <-chan func()) {
	created, err := lockStorage.CreateLock(ctx, lockName)
	if err != nil {
		log.Fatal("create lock error", err)
		return
	}
	if created {
		log.Printf("lock %s created", lockName)
	}

	isLockAcquired := false
	nextLockUpdateChan := time.After(0)

	for {
		select {
		case <-nextLockUpdateChan:
			curOwner, curTimeout, err := lockStorage.TryLock(ctx, lockName, ownerName, ttl)
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
			ch := time.After(ttl/10 + time.Duration(rand.Int63n(int64(ttl/10))))
			nextLockUpdateChan = ch

		case fn := <-funcsToRun:
			fn()

		case <-ctx.Done():
			return
		}
	}
}

func lockerContext(ctx context.Context, lockStorage LockStorage, lockName string, ownerName string, ttl time.Duration, lockCtxs chan context.Context, funcsToRun <-chan func()) {
	var masterDeadline atomic.Int64
	masterDeadline.Store(0)
	var wg sync.WaitGroup
	defer wg.Wait()
	lockAcquiringEvents := make(chan struct{}, 100)

	wg.Add(1)
	go func() {
		defer wg.Done()
		LockerThread(ctx, &masterDeadline, lockStorage, lockName, ownerName, ttl, lockAcquiringEvents, funcsToRun)
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

func LockerContext(ctx context.Context, lockStorage LockStorage, lockName string, ownerName string, ttl time.Duration, funcsToRun <-chan func()) chan context.Context {
	lockCtxs := make(chan context.Context, 100)

	go func() {
		defer close(lockCtxs)
		lockerContext(ctx, lockStorage, lockName, ownerName, ttl, lockCtxs, funcsToRun)
	}()

	return lockCtxs
}
