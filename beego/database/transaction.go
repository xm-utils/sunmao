package database

import (
	"context"
	"database/sql/driver"
	"fmt"
	"runtime/debug"

	"github.com/beego/beego/v2/client/orm"
)

type TxFunc func(o orm.TxOrmer) error

// Transaction 简化事务执行（推荐方式）：回调返回 error 或 panic 时回滚。
// 底层使用 orm.DoTx，崩溃与死锁类错误会自动重试。
func Transaction(fn func(o orm.TxOrmer) error) (err error) {
	defer recoverToErr(&err)
	if fn == nil {
		return orm.ErrArgs
	}
	return orm.NewOrm().DoTx(func(_ context.Context, txOrm orm.TxOrmer) error {
		return fn(txOrm)
	})
}

// Tx beego orm 事务处理器，与 gorm/database.Tx 用法一致（NewTx + Execute）。
type Tx struct {
	o   orm.TxOrmer
	err error
}

// NewTx 开启事务；开启失败时错误延后到 Execute 返回。
func NewTx() *Tx {
	tx := &Tx{}
	defer recoverToErr(&tx.err)
	ormer, err := orm.NewOrm().Begin()
	tx.o = ormer
	tx.err = err
	return tx
}

// Execute 在事务内执行回调，失败或 panic 时回滚。
func (tx *Tx) Execute(f TxFunc) (err error) {
	if tx.err != nil {
		return tx.err
	}
	if tx.o == nil {
		return driver.ErrBadConn
	}
	// 捕获 beego orm 或业务回调中可能出现的 panic，
	// 并在 panic 时回滚事务，避免连接泄漏。
	defer func() {
		if r := recover(); r != nil {
			_ = tx.o.Rollback()
			err = fmt.Errorf("panic recovered: %v\nstack: %s", r, debug.Stack())
		}
	}()
	err = f(tx.o)
	if err != nil {
		_ = tx.o.Rollback()
		return err
	}

	return tx.o.Commit()
}
