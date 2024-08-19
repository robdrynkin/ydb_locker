package ydb_locker

import (
	"context"
	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"log"
	"sync"
	"testing"
	"time"
)

func TestLocalLockerCtxSingleWorker(t *testing.T) {
	ctx := context.Background()
	storage := NewLocalLockStorage()
	locker := NewLocker(storage, "lock1", uuid.New().String(), time.Millisecond*100)

	ctx10s, cancel := context.WithTimeout(ctx, time.Second*1)
	defer cancel()

	cntr := 0

	for lockCtx := range locker.LockerContext(ctx10s) {
		for lockCtx.Err() == nil {
			cntr++
			log.Println("cntr:", cntr)
			time.Sleep(time.Millisecond * 100)
		}
	}

	if cntr < 8 || cntr > 12 {
		t.Errorf("expected 10, got %d", cntr)
	}
}

func TestYdbLockerCtxSingleWorker(t *testing.T) {
	ctx := context.Background()
	db := ConnectToDb(t, ctx)
	customReqBuilder := LockRequestBuilderImpl{
		"TestRunInLockerThreadSingleWorker",
		"lock_name_123",
		"owner_456",
		"deadline_789",
	}
	DropTableIfExists(t, ctx, db.Scripting(), customReqBuilder.TableName)
	if err := CreateLocksTable(ctx, db.Scripting(), &customReqBuilder); err != nil {
		t.Errorf("create table error: %v", err)
	}
	storage := YdbLockStorage{db, &customReqBuilder}
	locker := NewLocker(&storage, "lock1", uuid.New().String(), time.Second*10)

	ctx10s, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	cntr := 0

	for lockCtx := range locker.LockerContext(ctx10s) {
		for lockCtx.Err() == nil {
			cntr++
			log.Println("cntr:", cntr)
			time.Sleep(time.Second * 1)
		}
	}

	if cntr < 8 || cntr > 12 {
		t.Errorf("expected 10, got %d", cntr)
	}
}

func TestYdbLockerCtxMultipleWorkers(t *testing.T) {
	ctx := context.Background()
	db := ConnectToDb(t, ctx)
	reqBuilder := GetDefaultRequestBuilder("TestRunInLockerThreadMultipleWorkers")
	lockName := "lock2"

	DropTableIfExists(t, ctx, db.Scripting(), reqBuilder.TableName)
	if err := CreateLocksTable(ctx, db.Scripting(), reqBuilder); err != nil {
		t.Errorf("create table error: %v", err)
	}

	ctx10s, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	cntr := 0
	var wg sync.WaitGroup
	var curOwner string

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			storage := YdbLockStorage{db, reqBuilder}
			locker := NewLocker(&storage, lockName, uuid.New().String(), time.Second*10)
			defer wg.Done()

			lockCtxs := locker.LockerContext(ctx10s)

			for lockCtx := range lockCtxs {
				for lockCtx.Err() == nil {
					log.Println("owner:", locker.OwnerName, "cntr:", cntr)
					if curOwner == "" {
						curOwner = locker.OwnerName
					} else if curOwner != locker.OwnerName {
						t.Errorf("expected %s, got %s", curOwner, locker.OwnerName)
					}
					cntr++
					time.Sleep(time.Second * 1)
				}
			}
		}()
	}
	wg.Wait()

	if cntr < 8 || cntr > 12 {
		t.Errorf("expected 10, got %d", cntr)
	}
}

func TestYdbLockerCtxSingleWorkerLongTx(t *testing.T) {
	ctx := context.Background()
	db := ConnectToDb(t, ctx)
	customReqBuilder := LockRequestBuilderImpl{
		"TestRunInLockerThreadSingleWorker",
		"lock_name_123",
		"owner_456",
		"deadline_789",
	}
	DropTableIfExists(t, ctx, db.Scripting(), customReqBuilder.TableName)
	if err := CreateLocksTable(ctx, db.Scripting(), &customReqBuilder); err != nil {
		t.Errorf("create table error: %v", err)
	}
	storage := YdbLockStorage{db, &customReqBuilder}
	locker := NewLocker(&storage, "lock1", uuid.New().String(), time.Second*10)

	ctx10s, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	cntr := 0

	for lockCtx := range locker.LockerContext(ctx10s) {
		for lockCtx.Err() == nil {
			locker.ExecuteUnderLock(lockCtx, func(ctx context.Context, ts table.Session, txr table.Transaction) error {
				cntr++
				log.Println("cntr:", cntr)
				time.Sleep(time.Second * 1)
				_, err := txr.CommitTx(ctx)
				return err
			})
		}
	}

	if cntr < 8 || cntr > 12 {
		t.Errorf("expected 10, got %d", cntr)
	}
}
