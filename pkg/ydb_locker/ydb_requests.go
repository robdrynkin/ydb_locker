package ydb_locker

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"strings"
	"time"
)

func GetLockOwner(ctx context.Context, s table.Session, lockName string, reqBuilder LockRequestBuilder) (string, table.Transaction, error) {
	readOwnerTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()))
	query, params := reqBuilder.GetSelectLockQueryWithParams(lockName)
	txr, res, err := s.Execute(ctx, readOwnerTx, query, params)
	if err != nil {
		return "", txr, fmt.Errorf("execute error: %w", err)
	}
	if err = res.NextResultSetErr(ctx); err != nil {
		return "", txr, fmt.Errorf("next result set error: %w", err)
	}
	if !res.NextRow() {
		return "", txr, fmt.Errorf("no rows in result: %w", err)
	}
	var owner string
	err = res.ScanNamed(named.OptionalWithDefault(reqBuilder.GetOwnerColumnName(), &owner))
	if err != nil {
		return "", txr, fmt.Errorf("scan error: %w", err)
	}
	return owner, txr, nil
}

func CheckLockOwner(ctx context.Context, s table.Session, lockName string, expectedOwner string, reqBuilder LockRequestBuilder) (bool, table.Transaction, error) {
	owner, txr, err := GetLockOwner(ctx, s, lockName, reqBuilder)
	if err != nil {
		return false, txr, err
	}

	if owner != expectedOwner {
		return false, txr, txr.Rollback(ctx)
	}

	return true, txr, nil
}

func tryLock(ctx context.Context, s table.Session, lockName string, owner string, ttl time.Duration, reqBuilder LockRequestBuilder) (string, time.Time, error) {
	query, params := reqBuilder.GetUpdateLockQueryWithParams(lockName, owner, ttl)
	_, res, err := s.Execute(ctx, table.DefaultTxControl(), query, params)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("execute error: %w", err)
	}
	defer res.Close()
	if err = res.NextResultSetErr(ctx); err != nil {
		return "", time.Time{}, fmt.Errorf("next result set error: %w", err)
	}
	if !res.NextRow() {
		return "", time.Time{}, fmt.Errorf("no rows in result: %w", err)
	}
	var newOwner string
	var newDeadline time.Time
	err = res.ScanNamed(
		named.OptionalWithDefault(reqBuilder.GetOwnerColumnName(), &newOwner),
		named.OptionalWithDefault(reqBuilder.GetDeadlineColumnName(), &newDeadline),
	)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("scan error: %w", err)
	}
	return newOwner, newDeadline, nil
}

func TryLock(ctx context.Context, c table.Client, lockName string, ownerName string, ttl time.Duration, reqBuilder LockRequestBuilder) (string, time.Time, error) {
	var curOwner string
	var curTimeout time.Time

	err := c.Do(ctx, func(ctx context.Context, s table.Session) error {
		var err error
		curOwner, curTimeout, err = tryLock(ctx, s, lockName, ownerName, ttl, reqBuilder)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return "", time.Time{}, err
	}
	return curOwner, curTimeout, nil
}

func CreateLock(ctx context.Context, c table.Client, lockName string, reqBuilder LockRequestBuilder) (created bool, err error) {
	query, params := reqBuilder.GetCreateLockQueryWithParams(lockName)

	err = c.Do(ctx, func(ctx context.Context, s table.Session) error {
		_, _, err := s.Execute(ctx, table.DefaultTxControl(), query, params)
		if err == nil {
			created = true
		} else if ydb.IsOperationError(err, Ydb.StatusIds_PRECONDITION_FAILED) {
			if strings.Contains(err.Error(), "Conflict with existing key") {
				return nil
			}
		}

		return err
	})

	return
}

func CreateLocksTable(ctx context.Context, c scripting.Client, reqBuilder LockSchemaRequestBuilder) error {
	q := reqBuilder.GetCreateLocksTableQuery()
	_, err := c.Execute(ctx, q, nil)
	return err
}
