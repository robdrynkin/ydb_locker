package ydb_locker

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
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

func DropTableIfExists(t *testing.T, ctx context.Context, c scripting.Client, tableName string) {
	q := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	_, err := c.Execute(ctx, q, nil)
	if err != nil {
		t.Fatal("drop table error", err)
	}
}

func simpleTryLockCheck(t *testing.T, ctx context.Context, db *ydb.Driver, lockName string, ownerName string, reqBuilder LockRequestBuilder) {
	ttl := 10 * time.Second
	_, _, err := TryLock(ctx, db.Table(), lockName, ownerName, ttl, reqBuilder)
	if err != nil {
		t.Fatal("try lock error", err)
	}

	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		realOwner, tx, err := GetLockOwner(ctx, s, "lock1", reqBuilder)
		if err != nil {
			return err
		}
		if realOwner != ownerName {
			t.Errorf("wrong owner: %s != %s", realOwner, ownerName)
		}
		tx.CommitTx(ctx)
		return nil
	})
	if err != nil {
		t.Fatal("check lock owner error", err)
	}
}

func TestAcquireLock(t *testing.T) {
	ctx := context.Background()
	db := ConnectToDb(t, ctx)
	tableName := "TestAcquireLock"
	reqBuilder := GetDefaultRequestBuilder(tableName)

	DropTableIfExists(t, ctx, db.Scripting(), tableName)
	if err := CreateLocksTable(ctx, db.Scripting(), reqBuilder); err != nil {
		t.Fatal("create table error", err)
	}

	_, err := CreateLock(ctx, db.Table(), "lock1", reqBuilder)
	if err != nil {
		t.Fatal("create lock error", err)
	}

	simpleTryLockCheck(t, ctx, db, "lock1", "owner1", reqBuilder)
}

func TestAcquireLockWithExistingTable(t *testing.T) {
	ctx := context.Background()
	db := ConnectToDb(t, ctx)
	reqBuilder := &LockRequestBuilderImpl{
		TableName:          "TestAcquireLockWithExistingTable",
		LockNameColumnName: "lock_name",
		OwnerColumnName:    "owner",
		DeadlineColumnName: "deadline",
	}

	DropTableIfExists(t, ctx, db.Scripting(), reqBuilder.TableName)
	_, err := db.Scripting().Execute(ctx, fmt.Sprintf(`
		CREATE TABLE %[1]s (
		    %[2]s utf8, %[5]s string, %[3]s utf8, %[4]s timestamp,
			primary key (%[2]s)
		)
	`, reqBuilder.TableName, reqBuilder.LockNameColumnName, reqBuilder.OwnerColumnName, reqBuilder.DeadlineColumnName, "asdfgh"), nil)
	if err != nil {
		t.Fatal("create table error", err)
	}

	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		query := fmt.Sprintf("INSERT INTO %[1]s (%[2]s, asdfgh) VALUES ('lock1'u, 'qwerty')", reqBuilder.TableName, reqBuilder.LockNameColumnName)
		_, _, err := s.Execute(ctx, table.DefaultTxControl(), query, nil)
		return err
	})
	if err != nil {
		t.Fatal("insert into table error", err)
	}

	simpleTryLockCheck(t, ctx, db, "lock1", "owner1", reqBuilder)

}
