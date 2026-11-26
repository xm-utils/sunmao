package database

import (
	"context"
	"errors"
	"time"

	"github.com/beego/beego/v2/client/orm"
	"github.com/beego/beego/v2/client/orm/clauses/order_clause"

	"github.com/xm-utils/sunmao/common"
)

// PageParam 分页参数契约，*common.PageParam 即满足。
type PageParam interface {
	IsValid() bool
	GetLimit() (limit, offset int)
}

// TimeParam 时间区间参数契约，*common.TimeParam 即满足。
// 时间列名由 ListParam.TimeColumn 或 CtxBuilder.TimeColumn 单独给出，
// 因为 common 的实现不携带列名。
type TimeParam interface {
	IsValid() bool
	GetTime() (start, end time.Time)
}

// ListParam 列表查询参数。字段命名与 gorm/database.ListParam 对齐，
// 差异仅在于 Cond 沿用 beego 的 *orm.Condition，且多一个 TimeColumn 指定时间列。
type ListParam struct {
	Query      map[string]interface{} // 等值条件，键为 beego 表达式（如 "name__contains"）
	Cond       *orm.Condition         // 复合条件（AND/OR 嵌套）
	Page       PageParam
	Time       TimeParam
	TimeColumn string   // Time 生效的列名；设置了 Time 但未设置列名时直接报错，避免拼出非法表达式
	Order      []string // "-id" 为降序，"id" 为升序
}

// ErrTimeColumn 表示设置了时间区间却没有指定时间列。
var ErrTimeColumn = errors.New("beego/database: Time 条件需要指定 TimeColumn")

// CtxBuilder 链式查询构造器，方法集与 gorm/database.CtxBuilder 对齐。
//
// ctx 的作用范围受 beego orm 限制：写操作（Insert/Update/Delete/Read）与 Raw/Exec
// 走 *WithCtx 变体，可被取消与超时约束；QueryTable 链路（FindAll/FindList/Count/
// UpdateByCondition/DeleteByCondition）无法传递 ctx——orm 自带的 QueryTableWithCtx
// 已标注 deprecated 且 ctx 不生效。
type CtxBuilder[T any] struct {
	ctx        context.Context
	tx         orm.TxOrmer
	cond       *orm.Condition
	query      map[string]interface{}
	timeRange  TimeParam
	timeColumn string
	orders     []string
	clauses    []*order_clause.Order
	limit      int
	offset     int
	hasPage    bool
	selects    []string
	model      *T
}

// Ctx 创建一个绑定 ctx 的构造器。ctx 为 nil 时退回 context.Background()。
func Ctx[T any](ctx context.Context) *CtxBuilder[T] {
	if ctx == nil {
		ctx = context.Background()
	}
	return &CtxBuilder[T]{ctx: ctx}
}

// WithContext 替换上下文。
func (b *CtxBuilder[T]) WithContext(ctx context.Context) *CtxBuilder[T] {
	if ctx != nil {
		b.ctx = ctx
	}
	return b
}

// Tx 绑定事务内的 Ormer；不调用则每次操作使用独立的 orm.NewOrm()。
func (b *CtxBuilder[T]) Tx(tx orm.TxOrmer) *CtxBuilder[T] {
	b.tx = tx
	return b
}

// Model 指定查询主体，影响表名与条件字段的解析。
func (b *CtxBuilder[T]) Model(m *T) *CtxBuilder[T] {
	b.model = m
	return b
}

// Where 追加条件，多个条件按 AND 合并；nil 与空条件静默忽略。
func (b *CtxBuilder[T]) Where(conds ...*orm.Condition) *CtxBuilder[T] {
	for _, c := range conds {
		if c == nil || c.IsEmpty() || c == b.cond {
			continue
		}
		if b.cond == nil {
			b.cond = c
			continue
		}
		b.cond = b.cond.AndCond(c)
	}
	return b
}

// Query 追加等值条件，键为 beego 表达式（如 "status__in"），值为参数。
func (b *CtxBuilder[T]) Query(query map[string]interface{}) *CtxBuilder[T] {
	for k, v := range query {
		if b.query == nil {
			b.query = make(map[string]interface{}, len(query))
		}
		b.query[k] = v
	}
	return b
}

// Time 追加时间区间条件；nil 或无效的 TimeParam 静默忽略。
func (b *CtxBuilder[T]) Time(t TimeParam) *CtxBuilder[T] {
	if t != nil && t.IsValid() {
		b.timeRange = t
	}
	return b
}

// TimeColumn 指定 Time 条件作用的列名，默认列名为 created_at。
func (b *CtxBuilder[T]) TimeColumn(column string) *CtxBuilder[T] {
	if column != "" {
		b.timeColumn = column
	}
	return b
}

// Order 追加排序字段，前缀 "-" 表示降序（如 "-id"）。
func (b *CtxBuilder[T]) Order(orders ...string) *CtxBuilder[T] {
	b.orders = append(b.orders, orders...)
	return b
}

// OrderClauses 以 beego 的结构化排序子句排序，与 Order 同时给出时本方法优先。
func (b *CtxBuilder[T]) OrderClauses(orders ...*order_clause.Order) *CtxBuilder[T] {
	b.clauses = append(b.clauses, orders...)
	return b
}

// Page 设置分页；无效的 PageParam 会被忽略（不分页）。
func (b *CtxBuilder[T]) Page(p PageParam) *CtxBuilder[T] {
	if p != nil && p.IsValid() {
		b.limit, b.offset = p.GetLimit()
		b.hasPage = true
	}
	return b
}

// Select 指定读取的列，仅影响返回结构体字段的查询（FindAll/FindList/One）。
func (b *CtxBuilder[T]) Select(columns ...string) *CtxBuilder[T] {
	b.selects = append(b.selects, columns...)
	return b
}

// ApplyListParam 一次性应用 ListParam 的全部条件，便于参数对象直接透传。
func (b *CtxBuilder[T]) ApplyListParam(param ListParam) *CtxBuilder[T] {
	return b.Where(param.Cond).
		Query(param.Query).
		TimeColumn(param.TimeColumn).
		Time(param.Time).
		Order(param.Order...).
		Page(param.Page)
}

// ormer 返回当前操作使用的执行器：事务内优先。
// 取 QueryExecutor 而非 Ormer，因为 orm.TxOrmer 并不实现 Ormer（缺少 Begin/DoTx）。
func (b *CtxBuilder[T]) ormer() orm.QueryExecutor {
	if b.tx != nil {
		return b.tx
	}
	return orm.NewOrm()
}

// hasCondition 判断是否存在有效的 WHERE 条件（复合条件或等值条件）。
func (b *CtxBuilder[T]) hasCondition() bool {
	if b.cond != nil && !b.cond.IsEmpty() {
		return true
	}
	return len(b.query) > 0
}

// seter 构造 QuerySeter 并应用 Cond/Query/Time/Order/Page。
func (b *CtxBuilder[T]) seter() (orm.QuerySeter, error) {
	q := b.ormer().QueryTable(b.modelOrNew())
	if b.cond != nil && !b.cond.IsEmpty() {
		q = q.SetCond(b.cond)
	}
	for key, val := range b.query {
		q = q.Filter(key, val)
	}
	if b.timeRange != nil && b.timeRange.IsValid() {
		column := b.timeColumn
		if column == "" {
			return nil, ErrTimeColumn
		}
		start, end := b.timeRange.GetTime()
		// 结束时间只传日期（解析结果为当天 00:00:00）时补全到当天末尾，
		// 保持 __lte 右闭语义不漏掉当天整天的数据。
		if end.Hour() == 0 && end.Minute() == 0 && end.Second() == 0 && end.Nanosecond() == 0 {
			end = common.EndByTime(end)
		}
		q = q.Filter(column+"__gte", start).Filter(column+"__lte", end)
	}
	if len(b.clauses) > 0 {
		q = q.OrderClauses(b.clauses...)
	} else if len(b.orders) > 0 {
		q = q.OrderBy(b.orders...)
	}
	if b.hasPage {
		q = q.Limit(b.limit, b.offset)
	}
	return q, nil
}

// ReadOne 按主键读取单条记录（可带 cols 限定列）；未命中返回 (nil, nil)，
// 与 gorm/database 的 ReadOne 返回契约一致（链式条件不参与本方法）。
func (b *CtxBuilder[T]) ReadOne(t T, cols ...string) (result *T, err error) {
	defer recoverToErr(&err)
	v := t
	if err = b.ormer().ReadWithCtx(b.ctx, &v, cols...); err != nil {
		if errors.Is(err, orm.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &v, nil
}

// FindOne 按主键列 id 查询单条记录，与已有链式条件 AND 组合；未命中返回 ErrNoRows。
// 主键条件只作用于本次查询，不会残留到构造器上影响后续调用。
func (b *CtxBuilder[T]) FindOne(id int64) (result *T, err error) {
	defer recoverToErr(&err)
	cond := orm.NewCondition().And("id__exact", id)
	if b.cond != nil && !b.cond.IsEmpty() {
		cond = cond.AndCond(b.cond)
	}
	sub := *b
	sub.cond = cond
	q, err := sub.seter()
	if err != nil {
		return nil, err
	}
	var model T
	if err = q.One(&model, b.selects...); err != nil {
		return nil, err
	}
	return &model, nil
}

// modelOrNew 返回查询主体，未显式 Model 时用新的零值。
func (b *CtxBuilder[T]) modelOrNew() interface{} {
	if b.model != nil {
		return b.model
	}
	return new(T)
}

// FindAll 分页查询列表，同时返回总数；总数为 0 时不发起数据查询。
func (b *CtxBuilder[T]) FindAll() (list []*T, total int64, err error) {
	defer recoverToErr(&err)
	q, err := b.seter()
	if err != nil {
		return nil, 0, err
	}
	total, err = q.Count()
	if err != nil {
		return nil, 0, err
	}
	if total == 0 {
		return make([]*T, 0), 0, nil
	}
	list = make([]*T, 0)
	_, err = q.All(&list, b.selects...)
	return list, total, err
}

// FindList 查询列表（不统计总数，分页仍由 Page 决定）。
func (b *CtxBuilder[T]) FindList() (list []*T, err error) {
	defer recoverToErr(&err)
	q, err := b.seter()
	if err != nil {
		return nil, err
	}
	list = make([]*T, 0)
	_, err = q.All(&list, b.selects...)
	return list, err
}

// Count 统计满足条件的记录数。
func (b *CtxBuilder[T]) Count() (count int64, err error) {
	defer recoverToErr(&err)
	q, err := b.seter()
	if err != nil {
		return 0, err
	}
	return q.Count()
}

// Insert 插入一条记录，返回自增主键写入 data。
func (b *CtxBuilder[T]) Insert(data *T) (err error) {
	defer recoverToErr(&err)
	_, err = b.ormer().InsertWithCtx(b.ctx, data)
	return err
}

// InsertBatch 批量插入，batchSize 为单批条数（<=0 时整个列表一次写入），返回影响行数。
func (b *CtxBuilder[T]) InsertBatch(batchSize int, dataList []*T) (affected int64, err error) {
	defer recoverToErr(&err)
	if len(dataList) == 0 {
		return 0, nil
	}
	if batchSize <= 0 {
		batchSize = len(dataList)
	}
	return b.ormer().InsertMultiWithCtx(b.ctx, batchSize, dataList)
}

// Update 更新 data（按主键定位）。columns 非空时只写这些列，为空时写全部字段——
// 不同于 gorm/database 的“仅写非零字段”，需要同样的增量语义请用 UpdateModel。
func (b *CtxBuilder[T]) Update(data *T, columns ...string) (err error) {
	defer recoverToErr(&err)
	if data == nil {
		return orm.ErrArgs
	}
	_, err = b.ormer().UpdateWithCtx(b.ctx, data, columns...)
	return err
}

// UpdateModel 比对 old 与 form，只把 form 中非零且发生变化的字段写库。
// 无差异时直接返回 nil；old 为空指针时返回 ErrNoRows。
func (b *CtxBuilder[T]) UpdateModel(old *T, form T) (err error) {
	defer recoverToErr(&err)
	if old == nil {
		return orm.ErrNoRows
	}
	var fields []string
	old, fields = common.CompareAndAssign(old, form)
	if len(fields) == 0 {
		return nil
	}
	return b.Update(old, fields...)
}

// UpdateByCondition 按链式条件批量更新；updates 或条件为空时拒绝执行，避免全表更新。
func (b *CtxBuilder[T]) UpdateByCondition(updates orm.Params) (err error) {
	defer recoverToErr(&err)
	if len(updates) == 0 {
		return orm.ErrArgs
	}
	if !b.hasCondition() {
		return errors.New("beego/database: 条件为空，拒绝全表更新")
	}
	q, err := b.seter()
	if err != nil {
		return err
	}
	_, err = q.Update(updates)
	return err
}

// Delete 按 data 的主键删除。
func (b *CtxBuilder[T]) Delete(data *T) (err error) {
	defer recoverToErr(&err)
	if data == nil {
		return orm.ErrArgs
	}
	_, err = b.ormer().DeleteWithCtx(b.ctx, data)
	return err
}

// DeleteByCondition 按链式条件删除；条件为空时拒绝执行，避免全表删除。
func (b *CtxBuilder[T]) DeleteByCondition() (err error) {
	defer recoverToErr(&err)
	if !b.hasCondition() {
		return errors.New("beego/database: 条件为空，拒绝全表删除")
	}
	q, err := b.seter()
	if err != nil {
		return err
	}
	_, err = q.Delete()
	return err
}

// Raw 执行原生查询并映射到 []*T。
func (b *CtxBuilder[T]) Raw(sql string, values ...interface{}) (list []*T, err error) {
	defer recoverToErr(&err)
	list = make([]*T, 0)
	_, err = b.ormer().RawWithCtx(b.ctx, sql, values...).QueryRows(&list)
	return list, err
}

// Exec 执行原生写入语句，返回影响行数。
func (b *CtxBuilder[T]) Exec(sql string, values ...interface{}) (affected int64, err error) {
	defer recoverToErr(&err)
	res, err := b.ormer().RawWithCtx(b.ctx, sql, values...).Exec()
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
