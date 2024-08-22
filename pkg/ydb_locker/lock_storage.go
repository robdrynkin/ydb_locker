package ydb_locker

import (
	"context"
	"errors"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"sync"
	"time"
)

type LockStorage interface {
	CreateLock(ctx context.Context, lockName string) (bool, error)
	TryLock(ctx context.Context, lockName string, ownerName string, ttl time.Duration) (string, time.Time, error)
	CheckLockOwner(ctx context.Context, ts table.Session, lockName string, ownerName string) (bool, table.Transaction, error)
	ExecuteUnderLock(ctx context.Context, lockName string, ownerName string, f func(ctx context.Context, ts table.Session, tx table.Transaction) error) error
}

type YdbLockStorage struct {
	Db         *ydb.Driver
	ReqBuilder LockRequestBuilder
}

func (s *YdbLockStorage) CreateLock(ctx context.Context, lockName string) (bool, error) {
	return CreateLock(ctx, s.Db.Table(), lockName, s.ReqBuilder)
}

func (s *YdbLockStorage) TryLock(ctx context.Context, lockName string, ownerName string, ttl time.Duration) (string, time.Time, error) {
	return TryLock(ctx, s.Db.Table(), lockName, ownerName, ttl, s.ReqBuilder)
}

func (s *YdbLockStorage) CheckLockOwner(ctx context.Context, ts table.Session, lockName string, ownerName string) (bool, table.Transaction, error) {
	return CheckLockOwner(ctx, ts, lockName, ownerName, s.ReqBuilder)
}

func (s *YdbLockStorage) ExecuteUnderLock(ctx context.Context, lockName string, ownerName string, f func(ctx context.Context, ts table.Session, tx table.Transaction) error) error {
	return s.Db.Table().Do(ctx, func(ctx context.Context, ts table.Session) error {
		ok, tx, err := s.CheckLockOwner(ctx, ts, lockName, ownerName)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("not lock owner")
		}
		return f(ctx, ts, tx)
	})
}

type LocalLock struct {
	OwnerName string
	Deadline  time.Time
}

type LocalLockStorage struct {
	Locks map[string]*LocalLock
	Mu    sync.Mutex
}

func NewLocalLockStorage() *LocalLockStorage {
	return &LocalLockStorage{
		Locks: make(map[string]*LocalLock),
	}
}

func (s *LocalLockStorage) CreateLock(ctx context.Context, lockName string) (bool, error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if _, ok := s.Locks[lockName]; ok {
		return false, nil
	}
	s.Locks[lockName] = &LocalLock{}
	return true, nil
}

func (s *LocalLockStorage) TryLock(ctx context.Context, lockName string, ownerName string, ttl time.Duration) (string, time.Time, error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if lock, ok := s.Locks[lockName]; ok {
		if lock.OwnerName == ownerName {
			lock.Deadline = time.Now().Add(ttl)
		} else if lock.Deadline.Before(time.Now()) {
			lock.OwnerName = ownerName
			lock.Deadline = time.Now().Add(ttl)
		}
		return lock.OwnerName, lock.Deadline, nil
	}
	return "", time.Time{}, errors.New("lock not found")
}

func (s *LocalLockStorage) CheckLockOwner(ctx context.Context, ts table.Session, lockName string, ownerName string) (bool, table.Transaction, error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if lock, ok := s.Locks[lockName]; ok {
		return lock.OwnerName == ownerName, nil, nil
	}
	return false, nil, errors.New("lock not found")
}

func (s *LocalLockStorage) ExecuteUnderLock(ctx context.Context, lockName string, ownerName string, f func(ctx context.Context, ts table.Session, tx table.Transaction) error) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	if lock, ok := s.Locks[lockName]; ok {
		if lock.OwnerName == ownerName {
			return f(ctx, nil, nil)
		}
	}
	return errors.New("lock not found")
}
