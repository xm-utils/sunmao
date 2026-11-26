# 快速开始

> 5 分钟跑通基于 beego orm 的泛型 CRUD。完整能力见 [README.md](./README.md)。

## 1. 引入模块

```bash
go get github.com/xm-utils/sunmao/beego/database
```

## 2. 定义模型并初始化

```go
package main

import (
	"time"

	"github.com/beego/beego/v2/client/orm"
	"github.com/xm-utils/sunmao/beego/database"
)

type User struct {
	Id        int64     `orm:"pk;auto"`
	Name      string    `orm:"size(64)"`
	Status    int       `orm:"default(0)"`
	CreatedAt time.Time `orm:"auto_now_add;type(datetime)"`
}

func (u *User) TableName() string { return "users" }

func main() {
	if err := database.InitMysql(&database.MysqlConfig{
		Alias:    "default",
		Name:     "mydb",
		User:     "root",
		Password: "password",
		Host:     "127.0.0.1",
		Port:     "3306",
		Debug:    "true", // 字符串，非 bool
	}); err != nil {
		panic(err)
	}

	orm.RegisterModel(new(User))
	// 仅用于本地/测试环境快速建表，生产建议交给迁移工具
	if err := orm.RunSyncdb("default", false, true); err != nil {
		panic(err)
	}
}
```

配置字段与结构体 tag 的对应关系见 [config.example.yaml](./config.example.yaml)。

## 3. 增删改查

以下片段在第 2 节导入的基础上，另需 `context` 与 `github.com/xm-utils/sunmao/common`。

```go
ctx := context.Background()

// 插入：自增主键写回（MySQL 驱动）
u := &User{Name: "nobody", Status: 1}
if err := database.Ctx[User](ctx).Insert(u); err != nil {
	panic(err)
}

// 按主键取一条；未命中返回 (nil, nil)
got, err := database.Ctx[User](ctx).ReadOne(User{Id: u.Id})

// 按主键取一条；未命中返回 ErrNoRows
one, err := database.Ctx[User](ctx).FindOne(u.Id)

// 条件 + 排序 + 分页，同时拿到总数
cond := orm.NewCondition().And("status__exact", 1)
list, total, err := database.Ctx[User](ctx).
	Where(cond).
	Order("-created_at").
	Page(&common.PageParam{Page: 1, PageSize: 20}).
	FindAll()

// 差异更新：只写变化过的非零字段
old := *got
_ = database.Ctx[User](ctx).UpdateModel(&old, User{Status: 2})

// 按条件批量更新 / 删除：条件为空会被拒绝
_ = database.Ctx[User](ctx).
	Where(orm.NewCondition().And("status__exact", 0)).
	UpdateByCondition(orm.Params{"status": 1})
_ = database.Ctx[User](ctx).
	Where(orm.NewCondition().And("status__exact", 9)).
	DeleteByCondition()
```

不想用链式时，全部操作都有同名包级函数，参数顺序一致：

```go
list, total, err := database.FindAll[User](database.ListParam{
	Cond:       orm.NewCondition().And("status__exact", 1),
	Time:       &common.TimeParam{StartTime: "2026-11-01", EndTime: "2026-11-27"},
	TimeColumn: "created_at",
	Page:       &common.PageParam{Page: 1, PageSize: 20},
	Order:      []string{"-created_at"},
})

_ = database.Insert[User](nil, &User{Name: "nobody"})  // 第一个参数 nil 表示不在事务内
```

## 4. 时间区间

时间列由 `TimeColumn` 给出，区间为右闭：只填日期（如 `2026-11-27`）时自动补到当天 `23:59:59`，不会漏掉当天的数据。设置了 `Time` 却没给列名会返回 `ErrTimeColumn`，而不是拼出非法表达式。

## 5. 事务

```go
err := database.Transaction(func(o orm.TxOrmer) error {
	if _, e := o.Insert(&User{Name: "a", Status: 1}); e != nil {
		return e
	}
	return database.Ctx[User](ctx).Tx(o).Insert(&User{Name: "b", Status: 1})
})
// 回调返回 error 或 panic 时自动回滚；死锁类错误由 orm.DoTx 重试
```

需要手动控制提交时机时用 `NewTx`：

```go
tx := database.NewTx()
err = tx.Execute(func(o orm.TxOrmer) error {
	return database.Ctx[User](ctx).Tx(o).Delete(&User{Id: 1})
})
```

## 6. 与 gorm/database 的配合

两个模块的方法名、参数顺序与返回契约一致，业务代码迁移时只需替换 import 与两处类型：条件类型（`*database.Condition` ↔ `*orm.Condition`）、事务句柄（`*gorm.DB` ↔ `orm.TxOrmer`）。`ReadOne` 与 `Update` 的语义差异见 README 对照表。

## 下一步

- 全部 API 与边界：[README.md](./README.md)
- 配置模板：[config.example.yaml](./config.example.yaml)
