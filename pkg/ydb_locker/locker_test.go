package ydb_locker

import (
	"context"
	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"log"
	"sync"
	"testing"
	"time"
)

func ConnectToDb(t *testing.T, ctx context.Context) *ydb.Driver {
	db, err := ydb.Open(ctx, sugar.DSN("localhost:2136", "local", false))
	if err != nil {
		t.Fatal("Db connection error", err)
	}
	return db
}

func TestRunInLockerThreadSingleWorker(t *testing.T) {
	ctx := context.Background()
	db := ConnectToDb(t, ctx)
	customReqBuilder := LockRequestBuilderImpl{
		"TestRunInLockerThreadSingleWorker",
		"lock_name_123",
		"owner_456",
		"deadline_789",
	}
	if err := CreateLocksTable(ctx, db.Scripting(), &customReqBuilder); err != nil {
		t.Errorf("create table error: %v", err)
	}
	locker := Locker{db, "lock1", uuid.New().String(), time.Second * 10, &customReqBuilder}

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

func TestRunInLockerThreadMultipleWorkers(t *testing.T) {
	ctx := context.Background()
	db := ConnectToDb(t, ctx)
	reqBuilder := GetDefaultRequestBuilder("TestRunInLockerThreadMultipleWorkers")
	lockName := "lock2"

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
			locker := Locker{db, lockName, uuid.New().String(), time.Second * 10, reqBuilder}
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

//func TestRunInLockerThreadMultipleWorkersWithTimeout(t *testing.T) {
//	ctx := context.Background()
//	Db := ConnectToDb(t, ctx)
//
//	TableName := "TestRunInLockerThreadMultipleWorkersWithTimeout"
//	LockName := "lock2"
//
//	ctx10s, cancel := context.WithTimeout(ctx, time.Second*15)
//	defer cancel()
//
//	cntr := 0
//	var wg sync.WaitGroup
//
//	for i := 0; i < 10; i++ {
//		OwnerName := uuid.New().String()
//		wg.Add(1)
//		go func(i int) {
//			defer wg.Done()
//			workerCtx, cancel := context.WithTimeout(ctx10s, time.Duration(i+1)*time.Second)
//			defer cancel()
//			//RunInLockerThread(workerCtx, Db, TableName, LockName, OwnerName, func(deadline time.Time) {
//			//	log.Println("OwnerName: ", OwnerName, "deadline:", deadline)
//			//	cntr++
//			//	time.Sleep(time.Second * 1)
//			//})
//		}(i)
//		time.Sleep(time.Millisecond * 100)
//	}
//	wg.Wait()
//
//	if cntr < 8 || cntr > 12 {
//		t.Errorf("expected 10, got %d", cntr)
//	}
//}
