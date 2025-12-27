package dataset_log

import (
	"context"
	"reflect"
	"time"

	"github.com/didi/gendry/builder"
	"github.com/zly-app/zapp/log"
	"go.uber.org/zap"

	"github.com/zlyuancn/dataset_backend/client/db"
)

var (
	// 默认select当前表所有字段；
	// 如果在select时候传nil，会采用select * 执行
	// 这种方式会在库表新增字段的时候，由于两边结构字段未对齐而抛错！
	selectField = func() []string {
		var selectAllFields []string
		for _, field := range reflect.
			VisibleFields(reflect.TypeOf(Model{})) {
			// 拿到所有db字段
			if field.Tag.Get("db") != "" {
				selectAllFields = append(selectAllFields, field.Tag.Get("db"))
			}
		}
		return selectAllFields
	}()
)

const (
	tableName = "dataset_log"
)

type Model struct {
	ID         uint64 `db:"id"`
	DatasetId  uint64 `db:"dataset_id"`  // "数据集id"
	Remark     string `db:"remark"`      // "备注"
	Extend     string `db:"extend"`      // "扩展数据"
	LogType    byte   `db:"log_type"`    // "日志类型 枚举类型参考 pb.DataLogType"
	CreateTime int64  `db:"create_time"` // "创建时间, 微秒"
}

func MultiGet(ctx context.Context, where map[string]any) ([]*Model, error) {
	cond, vals, err := builder.BuildSelect(tableName, where, selectField)
	if err != nil {
		log.Error(ctx, "MultiGet BuildSelect err",
			zap.Any("where", where),
			zap.Error(err),
		)
		return nil, err
	}

	ret := []*Model{}
	err = db.GetLogSqlx().Find(ctx, &ret, cond, vals...)
	if err != nil {
		log.Error(ctx, "MultiGet Find fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
		return nil, err
	}
	return ret, nil
}

func Count(ctx context.Context, where map[string]any) (int64, error) {
	cond, vals, err := builder.BuildSelect(tableName, where, []string{"count(1)"})
	if err != nil {
		log.Error(ctx, "Count BuildSelect err",
			zap.Any("where", where),
			zap.Error(err),
		)
		return 0, err
	}

	var ret int64
	err = db.GetLogSqlx().FindOne(ctx, &ret, cond, vals...)
	if err != nil {
		log.Error(ctx, "Count FindOne fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
		return 0, err
	}
	return ret, nil
}

func MultiSave(ctx context.Context, list []*Model) (int64, error) {
	if len(list) == 0 {
		return 0, nil
	}
	var data []map[string]any
	for _, v := range list {
		d := map[string]any{
			"dataset_id": v.DatasetId,
			"remark":     v.Remark,
			"extend":     v.Extend,
			"log_type":   v.LogType,
		}
		if v.CreateTime > 0 {
			d["create_time"] = v.CreateTime
		} else {
			d["create_time"] = time.Now().UnixMicro()
		}
		data = append(data, d)
	}
	cond, vals, err := builder.BuildInsert(tableName, data)
	if err != nil {
		log.Error(ctx, "MultiSave call BuildInsert err", zap.Any("data", data), zap.Error(err))
		return 0, err
	}

	result, err := db.GetLogSqlx().Exec(ctx, cond, vals...)
	if err != nil {
		log.Error(ctx, "MultiSave call Exec fail.", zap.Error(err))
		return 0, err
	}
	return result.LastInsertId()
}
