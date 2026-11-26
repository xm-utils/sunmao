package database

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/beego/beego/v2/client/orm"
	_ "github.com/mattn/go-sqlite3"
	"github.com/xm-utils/sunmao/common"
)

type crudUser struct {
	Id        int64     `orm:"pk;auto"`
	Name      string    `orm:"size(64)"`
	Status    int       `orm:"default(0)"`
	Score     int64     `orm:"default(0)"`
	CreatedAt time.Time `orm:"auto_now_add;type(datetime)"`
}

func (u *crudUser) TableName() string { return "crud_users" }

func TestMain(m *testing.M) {
	if err := orm.RegisterDriver("sqlite3", orm.DRSqlite); err != nil {
		panic(err)
	}
	if err := orm.RegisterDataBase("default", "sqlite3", "file:curd_test?mode=memory&cache=shared"); err != nil {
		panic(err)
	}
	orm.RegisterModel(new(crudUser))
	if err := orm.RunSyncdb("default", false, true); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// resetUsers 清表并按给定行重新播种，返回每行的主键。
func resetUsers(t *testing.T, rows ...*crudUser) {
	t.Helper()
	o := orm.NewOrm()
	// beego 禁止无条件 QueryTable().Delete()，清表走原生 SQL。
	if _, err := o.Raw("DELETE FROM crud_users").Exec(); err != nil {
		t.Fatalf("clean table: %v", err)
	}
	for i := range rows {
		rows[i].Id = 0
		if _, err := o.Insert(rows[i]); err != nil {
			t.Fatalf("insert seed: %v", err)
		}
	}
}

func TestFindAllAppliesCondOrderAndPage(t *testing.T) {
	resetUsers(t,
		&crudUser{Name: "a", Status: 1, Score: 10},
		&crudUser{Name: "b", Status: 1, Score: 30},
		&crudUser{Name: "c", Status: 2, Score: 20},
		&crudUser{Name: "d", Status: 1, Score: 40},
	)

	cond := orm.NewCondition().And("status__exact", 1)
	list, total, err := Ctx[crudUser](context.Background()).
		Where(cond).
		Order("-score").
		Page(&common.PageParam{Page: 1, PageSize: 2}).
		FindAll()
	if err != nil {
		t.Fatalf("FindAll: %v", err)
	}
	if total != 3 {
		t.Fatalf("total should ignore page, want 3 got %d", total)
	}
	if len(list) != 2 || list[0].Name != "d" || list[1].Name != "b" {
		t.Fatalf("unexpected page content: %v", names(list))
	}
}

func TestWhereMergesConditionsByAnd(t *testing.T) {
	resetUsers(t,
		&crudUser{Name: "a", Status: 1, Score: 10},
		&crudUser{Name: "a", Status: 2, Score: 10},
		&crudUser{Name: "b", Status: 1, Score: 10},
	)

	list, err := Ctx[crudUser](context.Background()).
		Where(orm.NewCondition().And("name__exact", "a")).
		Where(orm.NewCondition().And("status__exact", 1)).
		FindList()
	if err != nil {
		t.Fatalf("FindList: %v", err)
	}
	if len(list) != 1 || list[0].Status != 1 {
		t.Fatalf("two Where should be AND-ed, got %v", names(list))
	}
}

func TestQueryMapAppliesEqualityFilters(t *testing.T) {
	resetUsers(t,
		&crudUser{Name: "a", Status: 1},
		&crudUser{Name: "b", Status: 1},
	)

	count, err := Ctx[crudUser](context.Background()).Query(map[string]interface{}{"name": "b"}).Count()
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Fatalf("want 1 got %d", count)
	}
}

func TestTimeRangeIsRightClosed(t *testing.T) {
	now := time.Date(2026, 11, 27, 15, 4, 5, 0, time.Local)
	earlier := now.Add(-48 * time.Hour)
	resetUsers(t,
		&crudUser{Name: "old", Status: 1, CreatedAt: earlier},
		&crudUser{Name: "today", Status: 1, CreatedAt: now},
	)

	// 结束时间只给日期（零点）：应补全到当天 23:59:59，把当天 15:04 的记录算进来。
	param := ListParam{
		Cond:       orm.NewCondition().And("status__exact", 1),
		Time:       &common.TimeParam{StartTime: "2026-11-26", EndTime: "2026-11-27"},
		TimeColumn: "created_at",
		Order:      []string{"id"},
	}
	list, total, err := Ctx[crudUser](context.Background()).ApplyListParam(param).FindAll()
	if err != nil {
		t.Fatalf("FindAll with time range: %v", err)
	}
	if total != 1 || len(list) != 1 || list[0].Name != "today" {
		t.Fatalf("right-closed range should keep today only, total=%d names=%v", total, names(list))
	}
}

func TestTimeWithoutColumnFailsExplicitly(t *testing.T) {
	_, _, err := Ctx[crudUser](context.Background()).
		Time(&common.TimeParam{StartTime: "2026-11-26", EndTime: "2026-11-27"}).
		FindAll()
	if !errors.Is(err, ErrTimeColumn) {
		t.Fatalf("want ErrTimeColumn, got %v", err)
	}
}

func TestOrderDescByPrefix(t *testing.T) {
	resetUsers(t,
		&crudUser{Name: "low", Score: 1},
		&crudUser{Name: "high", Score: 9},
	)

	list, err := Ctx[crudUser](context.Background()).Order("-score").FindList()
	if err != nil {
		t.Fatalf("FindList: %v", err)
	}
	if len(list) != 2 || list[0].Name != "high" {
		t.Fatalf(`"-score" should sort descending, got %v`, names(list))
	}
}

func TestFindOneByPrimaryKeyAndCombinedCond(t *testing.T) {
	resetUsers(t,
		&crudUser{Name: "a", Status: 1},
		&crudUser{Name: "b", Status: 2},
	)

	// 主键自增值不因 DELETE 重置，取实际写入的第一行主键。
	seeded, err := Ctx[crudUser](context.Background()).Order("id").FindList()
	if err != nil || len(seeded) != 2 {
		t.Fatalf("seed read: len=%d err=%v", len(seeded), err)
	}
	firstID := seeded[0].Id

	got, err := Ctx[crudUser](context.Background()).FindOne(firstID)
	if err != nil || got == nil || got.Name != "a" {
		t.Fatalf("FindOne(%d): got=%v err=%v", firstID, got, err)
	}
	// 已有链式条件与主键 AND 组合：状态不匹配时查不到。
	_, err = Ctx[crudUser](context.Background()).
		Where(orm.NewCondition().And("status__exact", 2)).
		FindOne(firstID)
	if !errors.Is(err, orm.ErrNoRows) {
		t.Fatalf("primary key should be AND-ed with chain condition, got %v", err)
	}
}

func TestReadOneMissReturnsNilWithoutError(t *testing.T) {
	resetUsers(t, &crudUser{Name: "a", Status: 1})

	seeded, err := Ctx[crudUser](context.Background()).Order("id").FindList()
	if err != nil || len(seeded) != 1 {
		t.Fatalf("seed read: len=%d err=%v", len(seeded), err)
	}

	hit, err := Ctx[crudUser](context.Background()).ReadOne(crudUser{Id: seeded[0].Id})
	if err != nil || hit == nil || hit.Name != "a" {
		t.Fatalf("ReadOne hit: got=%v err=%v", hit, err)
	}

	// 主键不存在：返回 (nil, nil)，与 gorm/database 的 ReadOne 契约一致。
	got, err := Ctx[crudUser](context.Background()).ReadOne(crudUser{Id: 999999})
	if err != nil {
		t.Fatalf("miss should not return error, got %v", err)
	}
	if got != nil {
		t.Fatalf("miss should return nil result, got %+v", got)
	}
}

func TestUpdateModelWritesOnlyChangedFields(t *testing.T) {
	seed := &crudUser{Name: "a", Status: 1, Score: 10}
	resetUsers(t, seed)

	old := &crudUser{Id: seed.Id}
	if err := orm.NewOrm().Read(old); err != nil {
		t.Fatalf("read seed: %v", err)
	}
	before := old.Score

	form := crudUser{Status: 5}
	if err := Ctx[crudUser](context.Background()).UpdateModel(old, form); err != nil {
		t.Fatalf("UpdateModel: %v", err)
	}

	after := &crudUser{Id: seed.Id}
	if err := orm.NewOrm().Read(after); err != nil {
		t.Fatalf("read after: %v", err)
	}
	if after.Status != 5 {
		t.Fatalf("status should be updated, got %d", after.Status)
	}
	if after.Score != before || after.Name != "a" {
		t.Fatalf("untouched fields should stay: name=%q score=%d", after.Name, after.Score)
	}
}

func TestUpdateModelNilOldReturnsErrNoRows(t *testing.T) {
	if err := Ctx[crudUser](context.Background()).UpdateModel(nil, crudUser{Status: 1}); !errors.Is(err, orm.ErrNoRows) {
		t.Fatalf("want ErrNoRows, got %v", err)
	}
}

func TestBatchOperationsRejectEmptyCondition(t *testing.T) {
	if err := Ctx[crudUser](context.Background()).UpdateByCondition(orm.Params{"status": 9}); err == nil {
		t.Fatal("empty condition must not update everything")
	}
	if err := Ctx[crudUser](context.Background()).DeleteByCondition(); err == nil {
		t.Fatal("empty condition must not delete everything")
	}
	if err := Ctx[crudUser](context.Background()).
		Where(orm.NewCondition()).UpdateByCondition(orm.Params{}); err == nil {
		t.Fatal("empty updates must be rejected")
	}
}

func TestUpdateByConditionAppliesChanges(t *testing.T) {
	resetUsers(t,
		&crudUser{Name: "a", Status: 1},
		&crudUser{Name: "b", Status: 1},
		&crudUser{Name: "c", Status: 2},
	)

	err := Ctx[crudUser](context.Background()).
		Where(orm.NewCondition().And("status__exact", 1)).
		UpdateByCondition(orm.Params{"status": 7})
	if err != nil {
		t.Fatalf("UpdateByCondition: %v", err)
	}
	count, err := Ctx[crudUser](context.Background()).
		Where(orm.NewCondition().And("status__exact", 7)).Count()
	if err != nil || count != 2 {
		t.Fatalf("want 2 rows updated, got %d err=%v", count, err)
	}
}

func TestInsertBatchAndDeleteByCondition(t *testing.T) {
	resetUsers(t)

	batch := []*crudUser{{Name: "x", Status: 3}, {Name: "y", Status: 3}, {Name: "z", Status: 4}}
	affected, err := Ctx[crudUser](context.Background()).InsertBatch(2, batch)
	if err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if affected != 3 {
		t.Fatalf("want 3 rows affected, got %d", affected)
	}
	// 主键回写依赖驱动能力：MySQL 下 beego 会回填递增 id，sqlite 批量插入不保证。
	written, err := Ctx[crudUser](context.Background()).
		Where(orm.NewCondition().And("status__gte", 3)).Count()
	if err != nil || written != 3 {
		t.Fatalf("batch rows should be persisted, got %d err=%v", written, err)
	}

	if err = Ctx[crudUser](context.Background()).
		Where(orm.NewCondition().And("status__exact", 3)).DeleteByCondition(); err != nil {
		t.Fatalf("DeleteByCondition: %v", err)
	}
	left, err := Ctx[crudUser](context.Background()).Count()
	if err != nil || left != 1 {
		t.Fatalf("want 1 row left, got %d err=%v", left, err)
	}
}

func TestRawAndExec(t *testing.T) {
	resetUsers(t, &crudUser{Name: "a", Status: 1})

	affected, err := Ctx[crudUser](context.Background()).Exec("UPDATE crud_users SET score = ? WHERE name = ?", 66, "a")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if affected != 1 {
		t.Fatalf("want 1 row affected, got %d", affected)
	}

	list, err := Ctx[crudUser](context.Background()).Raw("SELECT id, name, score FROM crud_users WHERE score = ?", 66)
	if err != nil {
		t.Fatalf("Raw: %v", err)
	}
	if len(list) != 1 || list[0].Score != 66 || list[0].Name != "a" {
		t.Fatalf("unexpected raw result: %v", list)
	}
}

func TestTransactionRollsBackOnError(t *testing.T) {
	resetUsers(t)

	err := Transaction(func(o orm.TxOrmer) error {
		if _, e := o.Insert(&crudUser{Name: "in-tx", Status: 1}); e != nil {
			return e
		}
		return errors.New("boom")
	})
	if err == nil {
		t.Fatal("transaction should return the callback error")
	}

	count, err := Ctx[crudUser](context.Background()).Count()
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 0 {
		t.Fatalf("rolled back insert must not be visible, got %d rows", count)
	}
}

func TestTxBuilderRunsInsideTransaction(t *testing.T) {
	resetUsers(t)

	tx := NewTx()
	err := tx.Execute(func(o orm.TxOrmer) error {
		return Ctx[crudUser](context.Background()).Tx(o).Insert(&crudUser{Name: "tx-row", Status: 1})
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	count, err := Ctx[crudUser](context.Background()).Count()
	if err != nil || count != 1 {
		t.Fatalf("committed row should be visible, got %d err=%v", count, err)
	}
}

func TestPanicIsConvertedToError(t *testing.T) {
	// crudUser 没有名为 missing_column 的字段，beego 在解析表达式时 panic。
	_, err := Ctx[crudUser](context.Background()).
		Where(orm.NewCondition().And("missing_column__exact", 1)).
		FindList()
	if err == nil {
		t.Fatal("panic from orm must surface as error")
	}
}

func names(list []*crudUser) []string {
	out := make([]string, 0, len(list))
	for _, u := range list {
		out = append(out, u.Name)
	}
	return out
}
