# Database Beego ORM 数据库工具库

> 模块：`github.com/xm-utils/sunmao/beego/database` ｜ 基于 beego v2 orm 的泛型 CRUD 封装

## 📋 目录

- [项目简介](#-项目简介)
- [与 gorm/database 的关系](#-与-gormdatabase-的关系)
- [安装](#-安装)
- [快速开始](#-快速开始)
- [核心功能](#-核心功能)
  - [两种调用形态](#两种调用形态)
  - [条件构造](#条件构造)
  - [分页与排序](#分页与排序)
  - [时间区间](#时间区间)
  - [更新与差异写回](#更新与差异写回)
  - [批量操作](#批量操作)
  - [事务](#事务)
  - [原生 SQL](#原生-sql)
  - [表名前缀](#表名前缀)
- [已知边界](#已知边界)

## 📖 项目简介

为仍在使用 beego orm 的服务提供与 `gorm/database` 一致的泛型数据访问手感：统一的 `Ctx[T](ctx)` 链式构造器 + 同名包级函数、`panic` 一律转为 `error`、条件为空时拒绝全表更新与删除。

业务代码在本模块与 `gorm/database` 之间迁移时，只有条件类型（`*orm.Condition` ↔ `*database.Condition`）与事务句柄（`orm.TxOrmer` ↔ `*gorm.DB`）两处需要改写，方法名、参数顺序与返回契约保持一致。

## ✨ 与 gorm/database 的关系

| 能力 | gorm/database | beego/database |
|---|---|---|
| 包级函数 | `ReadOne/FindOne/FindAll/FindList/Count/Insert/InsertBatch/Update/UpdateModel/UpdateByCondition/Delete/DeleteByCondition/Exec/Raw` | 同名同序，逐一行委托给构造器 |
| 链式构造器 | `Ctx[T](ctx)` | `Ctx[T](ctx)` |
| 条件类型 | 自带 `*Condition`（`Eq/In/Like/Between…`） | 沿用 beego `*orm.Condition` |
| 分页/时间契约 | `PageParam`/`TimeParam` 接口 | 同名接口，`*common.PageParam`、`*common.TimeParam` 直接满足 |
| 时间区间语义 | `>= start AND < end`，列名取自 `TimeParam.GetColumn()` | `>= start AND <= end`（纯日期补到当天 23:59:59），列名取自 `ListParam.TimeColumn` |
| `ReadOne` 语义 | 按结构体**非零字段**作为条件查询 | 按**主键**读取（beego `Read` 语义），未命中均返回 `(nil, nil)` |
| `Update` 写入范围 | 仅写非零字段（`Updates`） | 写全字段，或用 columns 指定列；需增量语义请用 `UpdateModel` |
| 读写分离 | `Primary()/Replica()` + DBResolver | 不提供，多库靠 `db_alias` 与 `orm.NewOrmUsingDB` |
| 事务 | `Transaction(fn)` / `NewTx().Execute(f)` | 同名同形，句柄为 `orm.TxOrmer` |
| 建表 | 依赖 `AutoMigrate` | 依赖 `orm.RunSyncdb` |

## 🚀 安装

```bash
go get github.com/xm-utils/sunmao/beego/database
```

依赖要求：Go 1.24.2+；数据库驱动 `github.com/go-sql-driver/mysql` 已由本模块内嵌注册。

## 🎯 快速开始

```go
package main

import (
	"context"
	"time"

	"github.com/beego/beego/v2/client/orm"
	"github.com/xm-utils/sunmao/beego/database"
	"github.com/xm-utils/sunmao/common"
)

type User struct {
	Id        int64     `orm:"pk;auto"`
	Name      string    `orm:"size(64)"`
	Status    int       `orm:"default(0)"`
	CreatedAt time.Time `orm:"auto_now_add;type(datetime)"`
}

func (u *User) TableName() string { return "users" }

func main() {
	err := database.InitMysql(&database.MysqlConfig{
		Alias:  "default",
		Name:   "mydb",
		User:   "root",
		Password: "password",
		Host:   "127.0.0.1",
		Port:   "3306",
	})
	if err != nil {
		panic(err)
	}
	orm.RegisterModel(new(User))
	// 建表（生产环境建议交给迁移工具）
	_ = orm.RunSyncdb("default", false, true)

	ctx := context.Background()
	_ = database.Ctx[User](ctx).Insert(&User{Name: "nobody", Status: 1})
}
```

## 🔧 核心功能

### 两种调用形态

包级函数适合无上下文的简单调用，链式构造器适合条件拼装与需要 ctx 的写入：

```go
// 包级函数
list, total, err := database.FindAll[User](database.ListParam{
	Cond:  orm.NewCondition().And("status__exact", 1),
	Page:  &common.PageParam{Page: 1, PageSize: 20},
	Order: []string{"-created_at"},
})

// 链式构造器，等价写法
list, total, err = database.Ctx[User](ctx).
	Where(orm.NewCondition().And("status__exact", 1)).
	Order("-created_at").
	Page(&common.PageParam{Page: 1, PageSize: 20}).
	FindAll()
```

### 条件构造

条件沿用 beego orm 的表达式语法（`字段__操作符`），多个 `Where` 之间按 AND 合并，`nil` 与空条件静默忽略：

```go
cond := orm.NewCondition().And("status__exact", 1).And("score__gte", 60)

database.Ctx[User](ctx).
	Where(cond).
	Where(orm.NewCondition().And("name__in", "a", "b")). // 与上一个条件 AND
	FindList()
```

`Query` 接受等值 map，键同样是 beego 表达式：

```go
database.Ctx[User](ctx).Query(map[string]interface{}{"name__contains": "ad"}).Count()
```

### 分页与排序

`Page` 接受任何实现 `IsValid()/GetLimit()` 的类型（`*common.PageParam` 即可）；`Order` 用字符串，前缀 `-` 表示降序。需要多列混合方向时可用 beego 的结构化子句：

```go
import "github.com/beego/beego/v2/client/orm/clauses/order_clause"

database.Ctx[User](ctx).
	Order("-score", "id").
	Page(&common.PageParam{Page: 2, PageSize: 10}).
	FindAll()

database.Ctx[User](ctx).OrderClauses(
	order_clause.NewOrder("status", order_clause.Asc),
	order_clause.NewOrder("created_at", order_clause.Desc),
).FindList()
```

`Order` 与 `OrderClauses` 同时给出时，后者优先。

### 时间区间

时间列由 `TimeColumn` 指定；未指定却带了 `Time` 时返回 `ErrTimeColumn`，不会拼出非法表达式。结束时间只给日期（解析结果为当天零点）时自动补到当天 `23:59:59`，区间为右闭：

```go
list, total, err := database.Ctx[User](ctx).
	Where(orm.NewCondition().And("status__exact", 1)).
	TimeColumn("created_at").
	Time(&common.TimeParam{StartTime: "2026-11-01", EndTime: "2026-11-27"}).
	FindAll()
```

需要严格的左闭右开区间时，绕过 `Time`，自行用 `__gte` / `__lt` 拼条件：

```go
start := time.Date(2026, 11, 1, 0, 0, 0, 0, time.Local)
end := time.Date(2026, 11, 27, 0, 0, 0, 0, time.Local)
database.Ctx[User](ctx).
	Where(orm.NewCondition().And("created_at__gte", start).And("created_at__lt", end)).
	FindList()
```

### 更新与差异写回

```go
// 指定列更新
_ = database.Ctx[User](ctx).Update(user, "status", "score")

// 只把 form 中非零且发生变化的字段写库，无差异时不发 SQL
_ = database.Ctx[User](ctx).UpdateModel(old, User{Status: 5})

// 按条件批量更新，条件或更新内容为空时拒绝执行
_ = database.Ctx[User](ctx).
	Where(orm.NewCondition().And("status__exact", 1)).
	UpdateByCondition(orm.Params{"status": 7})
```

### 批量操作

```go
affected, err := database.Ctx[User](ctx).InsertBatch(500, []*User{{Name: "a"}, {Name: "b"}})

err = database.Ctx[User](ctx).
	Where(orm.NewCondition().And("status__exact", 3)).
	DeleteByCondition()
```

### 事务

```go
// 推荐：回调返回 error 或 panic 时自动回滚，死锁类错误由 orm.DoTx 重试
err := database.Transaction(func(o orm.TxOrmer) error {
	if _, e := o.Insert(&User{Name: "a"}); e != nil {
		return e
	}
	return database.Ctx[User](ctx).Tx(o).Insert(&User{Name: "b"})
})

// 手动控制
tx := database.NewTx()
err = tx.Execute(func(o orm.TxOrmer) error {
	return database.Ctx[User](ctx).Tx(o).Update(user, "status")
})
```

构造器绑定事务句柄后，读写都走同一事务；`Tx(nil)` 退回全局连接。

### 原生 SQL

```go
affected, err := database.Ctx[User](ctx).Exec("UPDATE users SET score = ? WHERE id = ?", 66, 1)

rows, err := database.Ctx[User](ctx).Raw("SELECT id, name, score FROM users WHERE score > ?", 60)
```

`Raw` 的列名需与结构体字段映射一致，只取需要的列以避免空值覆盖。

### 表名前缀

```go
database.SetPrefix("app_")
database.TableName("users") // app_users
```

前缀读写由 `sync.RWMutex` 保护，可在运行期安全调整。

## ⚠️ 已知边界

- **ctx 的覆盖范围受 beego orm 限制**：`Insert/Update/Delete/Read/Raw` 走 `*WithCtx` 变体，可被取消与超时约束；`QueryTable` 链路（`FindAll/FindList/Count/UpdateByCondition/DeleteByCondition/FindOne`）无法传递 ctx——orm 自带的 `QueryTableWithCtx` 已标注 deprecated 且 ctx 不生效。
- **不提供读写分离**：需要主从或多库时，用不同的 `db_alias` 分别 `InitMysql`，再靠 `orm.NewOrmUsingDB(alias)` 选择；本模块不会自动路由。
- **`InsertBatch` 的主键回写依赖驱动**：MySQL 下 beego 会回填自增 id，SQLite 等驱动批量插入不保证回写。
- **`ReadOne` 按主键定位**：沿用 beego `Read` 的语义，而 `gorm/database` 的同名方法按非零字段匹配；需要按字段条件取单条时，用 `Where(...)` + `FindList()` 后自行取首条。
- **`Update` 写全字段**：不传 columns 时会把零值一并写入（gorm 侧仅写非零字段）；增量更新请用 `UpdateModel`。
- **`UpdateModel` 只按主键定位**：`gorm/database` 的同名方法额外接受 `whereCols` 追加 WHERE 条件，本模块无对应能力，需要时在调用方改用 `UpdateByCondition`。
- **条件类型仍为三方类型**：`ListParam.Cond`、`Where`、`UpdateByCondition` 的 map 直接使用 `*orm.Condition` 与 `orm.Params`，切换 ORM 时这一层需要改写。

## 📄 许可证

本项目遵循仓库根目录 [LICENSE](https://github.com/xm-utils/sunmao/blob/main/LICENSE) 中的条款。
