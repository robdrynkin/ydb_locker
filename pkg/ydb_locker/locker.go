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
}

func (l *Locker) CheckLockOwner(ctx context.Context, s table.Session) (bool, table.Transaction, error) {
	return l.LockStorage.CheckLockOwner(ctx, s, l.LockName, l.OwnerName)
}

func (l *Locker) LockerContext(ctx context.Context) chan context.Context {
	return LockerContext(ctx, l.LockStorage, l.LockName, l.OwnerName, l.Ttl)
}
