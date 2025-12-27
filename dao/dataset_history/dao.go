package dataset_history

import (
	"context"
	"errors"
	"time"

	"github.com/didi/gendry/builder"
	"github.com/zly-app/zapp/log"
	"go.uber.org/zap"

	"github.com/zlyuancn/dataset_backend/client/db"
)

const (
	tableName = "dataset_history"
)

type Model struct {
	DatasetId     uint      `db:"dataset_id"`     // "数据集id"
	DatasetName   string    `db:"dataset_name"`   // "数据集名"
	Remark        string    `db:"remark"`         // "备注"
	DatasetExtend string    `db:"dataset_extend"` // "数据集扩展数据"
	CreateTime    time.Time `db:"create_time"`
	OpSource      string    `db:"op_source"`    // "最后操作来源"
	OpUserId      string    `db:"op_user_id"`   // "最后操作用户id"
	OpUserName    string    `db:"op_user_name"` // "最后操作用户名"
	OpRemark      string    `db:"op_remark"`    // "最后操作备注"
	OpCmd         byte      `db:"op_cmd"`       // "操作指令"
	Status        byte      `db:"status"`       // "任务状态 0=已创建 2=运行中 3=已完成 4=正在停止 5=已停止 6=已删除"
	StatusInfo    string    `db:"status_info"`  // "状态信息"
}

func CreateOneModel(ctx context.Context, v *Model) (int64, error) {
	if v == nil {
		return 0, errors.New("CreateOneModel v is empty")
	}

	var data []map[string]any
	data = append(data, map[string]any{
		"dataset_id":     v.DatasetId,
		"dataset_name":   v.DatasetName,
		"remark":         v.Remark,
		"dataset_extend": v.DatasetExtend,
		"op_source":      v.OpSource,
		"op_user_id":     v.OpUserId,
		"op_user_name":   v.OpUserName,
		"op_remark":      v.Remark,
		"op_cmd":         v.OpCmd,
		"status":         v.Status,
		"status_info":    v.StatusInfo,
	})
	cond, vals, err := builder.BuildInsert(tableName, data)
	if err != nil {
		log.Error(ctx, "CreateOneModel BuildSelect err",
			zap.Any("data", data),
			zap.Error(err),
		)
		return 0, err
	}

	result, err := db.GetSqlx().Exec(ctx, cond, vals...)
	if err != nil {
		log.Error(ctx, "CreateOneModel fail.", zap.Any("data", data), zap.Error(err))
		return 0, err
	}
	return result.LastInsertId()
}
