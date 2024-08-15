package ydb_locker

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"time"
)

type Locker struct {
	Db        *ydb.Driver
	LockName  string
	OwnerName string
	Ttl       time.Duration

	RequestBuilder LockRequestBuilder
}

func (l *Locker) CheckLockOwner(ctx context.Context, s table.Session) (bool, table.Transaction, error) {
	return CheckLockOwner(ctx, s, l.LockName, l.OwnerName, l.RequestBuilder)
}

func (l *Locker) LockerContext(ctx context.Context) chan context.Context {
	return LockerContext(ctx, l.Db, l.LockName, l.OwnerName, l.Ttl, l.RequestBuilder)
}
