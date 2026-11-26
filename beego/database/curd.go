package database

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/beego/beego/v2/client/orm"
)

// recoverToErr 捕获 beego orm 调用中可能出现的 panic，并将其转换为 error 返回，
// 避免因底层 panic 直接导致程序崩溃。
func recoverToErr(err *error) {
	if r := recover(); r != nil {
		var e error
		switch v := r.(type) {
		case error:
			e = v
		default:
			e = fmt.Errorf("%v", v)
		}
		if err != nil {
			*err = fmt.Errorf("panic recovered: %w\nstack: %s", e, debug.Stack())
		}
	}
}

// 以下包级函数与 gorm/database 的同名函数一一对应，均为一行委托给
// CtxBuilder；除条件类型（*orm.Condition）与事务句柄（orm.TxOrmer）外，
// 签名、返回值与语义保持一致。tx 传 nil 表示使用全局连接而非事务。

// ReadOne 以入参结构体的非零字段作为条件读取单条记录，失败返回 nil。
func ReadOne[T any](t T, cols ...string) (result *T) {
	result, _ = Ctx[T](context.Background()).ReadOne(t, cols...)
	return
}

// FindOne 根据主键查询单条记录，未命中返回 nil。
func FindOne[T any](id int64) *T {
	result, _ := Ctx[T](context.Background()).FindOne(id)
	return result
}

// FindAll 分页查询列表，同时返回总数。
func FindAll[T any](param ListParam) (list []*T, total int64, err error) {
	return Ctx[T](context.Background()).ApplyListParam(param).FindAll()
}

// FindList 查询列表（不统计总数），可选传入分页参数。
func FindList[T any](cond *orm.Condition, page ...PageParam) (list []*T, err error) {
	b := Ctx[T](context.Background()).Where(cond)
	if len(page) > 0 {
		b = b.Page(page[0])
	}
	return b.FindList()
}

// Count 统计满足条件的记录数。
func Count[T any](cond *orm.Condition) (count int64, err error) {
	return Ctx[T](context.Background()).Where(cond).Count()
}

// Update 更新记录，columns 非空时只写这些列。
func Update[T any](tx orm.TxOrmer, data *T, columns ...string) (err error) {
	return Ctx[T](context.Background()).Tx(tx).Update(data, columns...)
}

// UpdateModel 比对 old 与 form，把 form 中非零且变化的字段写库（按主键定位）。
func UpdateModel[T any](tx orm.TxOrmer, old *T, form T) (err error) {
	return Ctx[T](context.Background()).Tx(tx).UpdateModel(old, form)
}

// UpdateByCondition 按条件批量更新；cond 为空时拒绝执行。
func UpdateByCondition[T any](tx orm.TxOrmer, cond *orm.Condition, updates orm.Params) (err error) {
	return Ctx[T](context.Background()).Tx(tx).Where(cond).UpdateByCondition(updates)
}

// Delete 按主键删除记录。
func Delete[T any](tx orm.TxOrmer, data *T) (err error) {
	return Ctx[T](context.Background()).Tx(tx).Delete(data)
}

// DeleteByCondition 按条件删除；cond 为空时拒绝执行。
func DeleteByCondition[T any](tx orm.TxOrmer, cond *orm.Condition) (err error) {
	return Ctx[T](context.Background()).Tx(tx).Where(cond).DeleteByCondition()
}

// Insert 插入记录，自增主键写回 data。
func Insert[T any](tx orm.TxOrmer, data *T) (err error) {
	return Ctx[T](context.Background()).Tx(tx).Insert(data)
}

// InsertBatch 批量插入，返回影响行数。
func InsertBatch[T any](tx orm.TxOrmer, batchSize int, dataList []*T) (affected int64, err error) {
	return Ctx[T](context.Background()).Tx(tx).InsertBatch(batchSize, dataList)
}

// Exec 执行原生写入语句。
func Exec(sql string, values ...interface{}) error {
	_, err := Ctx[any](context.Background()).Exec(sql, values...)
	return err
}

// Raw 执行原生查询并映射到 []*T。
func Raw[T any](sql string, values ...interface{}) (list []*T, err error) {
	return Ctx[T](context.Background()).Raw(sql, values...)
}
