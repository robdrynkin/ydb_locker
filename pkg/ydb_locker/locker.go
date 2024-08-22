package ydb_locker

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"time"
)

type Locker struct {
	LockStorage LockStorage
	LockName    string
	OwnerName   string
	Ttl         time.Duration

	FuncsToRun chan func()
}

func NewLocker(lockStorage LockStorage, lockName string, ownerName string, ttl time.Duration) *Locker {
	return &Locker{
		LockStorage: lockStorage,
		LockName:    lockName,
		OwnerName:   ownerName,
		Ttl:         ttl,
		FuncsToRun:  make(chan func(), 1000),
	}
}

func (l *Locker) ExecuteUnderLock(ctx context.Context, f func(context.Context, table.Session, table.Transaction) error) error {
	res := make(chan error, 1)
	l.FuncsToRun <- func() {
		res <- l.LockStorage.ExecuteUnderLock(ctx, l.LockName, l.OwnerName, f)
	}
	return <-res
}

func (l *Locker) LockerContext(ctx context.Context) chan context.Context {
	return LockerContext(ctx, l.LockStorage, l.LockName, l.OwnerName, l.Ttl, l.FuncsToRun)
}
