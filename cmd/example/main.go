package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"log"
	"time"
	"ydb_locker/pkg/ydb_locker"
)

func DoSomeUserStuff(ctx context.Context, s table.Session, txr table.Transaction) error {
	txr, res, err := s.Execute(ctx, table.TxControl(table.WithTx(txr)), "select * from locks", nil)
	if err != nil {
		return fmt.Errorf("execute error: %w", err)
	}
	defer res.Close()
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var lock_name string
			var owner string
			var deadline time.Time
			err = res.ScanNamed(named.OptionalWithDefault("lock_name", &lock_name),
				named.OptionalWithDefault("owner", &owner),
				named.OptionalWithDefault("deadline", &deadline))
			if err != nil {
				return fmt.Errorf("scan error: %w", err)
			}
			fmt.Println("lock_name:", lock_name, "owner:", owner, "timeout:", deadline)
		}
	}
	r, err := txr.CommitTx(ctx)
	defer r.Close()
	time.Sleep(time.Second * 1)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	var endpoint string
	var database string
	var tableName string

	flag.StringVar(&endpoint, "endpoint", "localhost:2136", "YDB endpoint")
	flag.StringVar(&database, "database", "local", "YDB database")
	flag.StringVar(&tableName, "table", "locks", "YDB table")
	flag.Parse()

	ctx := context.Background()

	log.Printf("connecting -> endpoint: %s, database: %s", endpoint, database)
	db, err := ydb.Open(ctx, sugar.DSN(endpoint, database, false))
	if err != nil {
		log.Fatal("Db connection error", err)
		return
	}

	reqBuilder := ydb_locker.GetDefaultRequestBuilder("locks")
	if err := ydb_locker.CreateLocksTable(ctx, db.Scripting(), reqBuilder); err != nil {
		log.Fatal("create table error", err)
		return
	}
	locker := ydb_locker.Locker{db, "lock1", "owner1", time.Second * 10, reqBuilder}
	locker.RunInLockerThread(ctx, func(deadline time.Time) {
		reqCtx, cancel := context.WithDeadline(ctx, deadline)
		defer cancel()

		err = db.Table().Do(reqCtx, func(ctx context.Context, s table.Session) error {
			ok, txr, err := locker.CheckLockOwner(ctx, s)
			if err != nil {
				return err
			}
			if !ok {
				fmt.Println("lock is not owned by", locker.OwnerName)
				return nil
			}

			return DoSomeUserStuff(ctx, s, txr)
		})

		if err != nil {
			log.Fatal("lock error", err)
			return
		}
	})

}
