package dataset_list

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/didi/gendry/builder"
	"github.com/zly-app/zapp/log"
	"go.uber.org/zap"

	"github.com/zlyuancn/dataset_backend/pb"

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
	tableName = "dataset_list"
)

type Model struct {
	DatasetId     uint      `db:"dataset_id"`     // "数据集id"
	DatasetName   string    `db:"dataset_name"`   // "数据集名"
	Remark        string    `db:"remark"`         // "备注"
	DatasetExtend string    `db:"dataset_extend"` // "数据集扩展数据"
	ChunkMeta     string    `db:"chunk_meta"`     // "所有chunk元数据"
	ValueTotal    int64     `db:"value_total"`    // "总数"
	CreateTime    time.Time `db:"create_time"`
	UpdateTime    time.Time `db:"update_time"`
	ProcessedTime int64     `db:"processed_time"` // "加工完成时间"
	OpSource      string    `db:"op_source"`      // "最后操作来源"
	OpUserId      string    `db:"op_user_id"`     // "最后操作用户id"
	OpUserName    string    `db:"op_user_name"`   // "最后操作用户名"
	OpRemark      string    `db:"op_remark"`      // "最后操作备注"
	Status        byte      `db:"status"`         // "任务状态 0=已创建 2=运行中 3=已完成 4=正在停止 5=已停止 6=已删除"
	StatusInfo    string    `db:"status_info"`    // "状态信息"
	ActivateTime  time.Time `db:"activate_time"`  // "最后激活时间"
}

func CreateOneModel(ctx context.Context, v *Model) (int64, error) {
	if v == nil {
		return 0, errors.New("CreateOneModel v is empty")
	}

	var data []map[string]any
	data = append(data, map[string]any{
		"dataset_name":   v.DatasetName,
		"remark":         v.Remark,
		"dataset_extend": v.DatasetExtend,
		"chunk_meta":     "",
		"op_source":      v.OpSource,
		"op_user_id":     v.OpUserId,
		"op_user_name":   v.OpUserName,
		"op_remark":      v.OpRemark,
		"status":         v.Status,
		"status_info":    v.StatusInfo,
		"activate_time":  time.Now(),
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

func GetOne(ctx context.Context, where map[string]any, selectField []string) (*Model, error) {
	if where == nil {
		where = map[string]any{}
	}
	where["_limit"] = []uint{1} // 限制只查询一条记录
	cond, vals, err := builder.BuildSelect(tableName, where, selectField)
	if err != nil {
		log.Error(ctx, "GetOne BuildSelect fail.", zap.Any("where", where), zap.Error(err))
		return nil, err
	}
	ret := Model{}
	err = db.GetSqlx().FindOne(ctx, &ret, cond, vals...)
	if err != nil {
		log.Error(ctx, "GetOne FindOne fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
		return nil, err
	}
	return &ret, nil
}

func GetOneByDatasetId(ctx context.Context, datasetId int) (*Model, error) {
	where := map[string]interface{}{
		"dataset_id": datasetId,
	}
	v, err := GetOne(ctx, where, selectField)
	if err != nil {
		log.Error(ctx, "GetOneByDatasetId fail.", zap.Error(err))
		return nil, err
	}
	return v, nil
}

func AdminUpdateDataset(ctx context.Context, v *Model, whereStatus byte) (int64, error) {
	if v == nil {
		return 0, errors.New("AdminUpdateDataset v is empty")
	}
	if v.DatasetId == 0 {
		return 0, errors.New("AdminUpdateDataset DatasetId is empty")
	}
	where := map[string]any{
		"dataset_id": v.DatasetId,
		"status":     whereStatus,
		"_limit":     1,
	}
	update := map[string]any{
		"dataset_name":  v.DatasetName,
		"remark":        v.Remark,
		"update_time":   time.Now(),
		"op_source":     v.OpSource,
		"op_user_id":    v.OpUserId,
		"op_user_name":  v.OpUserName,
		"op_remark":     v.OpRemark,
		"status_info":   v.StatusInfo,
		"activate_time": time.Now(),
	}
	if v.DatasetExtend != "" {
		update["dataset_extend"] = v.DatasetExtend
	}
	cond, vals, err := builder.BuildUpdate(tableName, where, update)
	if err != nil {
		log.Error(ctx, "AdminUpdateDataset BuildSelect fail.", zap.Any("where", where), zap.Any("update", update), zap.Error(err))
		return 0, err
	}

	result, err := db.GetSqlx().Exec(ctx, cond, vals...)
	if nil != err {
		log.Error(ctx, "AdminUpdateJob call Exec fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
		return 0, err
	}
	return result.RowsAffected()
}

// 仅更新状态和操作人相关信息
func AdminUpdateStatus(ctx context.Context, v *Model, whereStatus byte) (int64, error) {
	if v == nil {
		return 0, errors.New("AdminUpdateStatus v is empty")
	}
	if v.DatasetId == 0 {
		return 0, errors.New("AdminUpdateStatus DatasetId is empty")
	}
	const cond = `
update dataset_list
set 
    status=?,
    update_time=now(),
    op_source=?,
    op_user_id=?,
    op_user_name=?,
    op_remark=?,
    status_info=?,
    activate_time=now()
where dataset_id = ?
    and status = ?
limit 1;`
	vals := []interface{}{
		v.Status,
		v.OpSource,
		v.OpUserId,
		v.OpUserName,
		v.OpRemark,
		v.StatusInfo,
		v.DatasetId,
		whereStatus,
	}
	result, err := db.GetSqlx().Exec(ctx, cond, vals...)
	if nil != err {
		log.Error(ctx, "AdminUpdateStatus fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
		return 0, err
	}
	return result.RowsAffected()
}

func MultiGetBySelect(ctx context.Context, where map[string]any, selectField []string) ([]*Model, error) {
	cond, vals, err := builder.BuildSelect(tableName, where, selectField)
	if err != nil {
		log.Error(ctx, "MultiGetBySelect BuildSelect err",
			zap.Any("where", where),
			zap.Error(err),
		)
		return nil, err
	}

	ret := []*Model{}
	err = db.GetSqlx().Find(ctx, &ret, cond, vals...)
	if err != nil {
		log.Error(ctx, "MultiGetBySelect Find fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
		return nil, err
	}
	return ret, nil
}

func MultiGetId(ctx context.Context, where map[string]any) ([]uint, error) {
	selectField := []string{"dataset_id"}
	cond, vals, err := builder.BuildSelect(tableName, where, selectField)
	if err != nil {
		log.Error(ctx, "MultiGetId BuildSelect err",
			zap.Any("where", where),
			zap.Error(err),
		)
		return nil, err
	}

	ret := []uint{}
	err = db.GetSqlx().Find(ctx, &ret, cond, vals...)
	if err != nil {
		log.Error(ctx, "MultiGetId Find fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
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
	err = db.GetSqlx().FindOne(ctx, &ret, cond, vals...)
	if err != nil {
		log.Error(ctx, "Count FindOne fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
		return 0, err
	}
	return ret, nil
}

func UpdateOne(ctx context.Context, datasetId int, updateData map[string]interface{}, whereStatus byte) error {
	where := map[string]any{
		"dataset_id": datasetId,
		"_limit":     1,
	}
	if whereStatus > 0 {
		where["status"] = whereStatus
	}
	cond, vals, err := builder.BuildUpdate(tableName, where, updateData)
	if err != nil {
		log.Error(ctx, "UpdateOne BuildUpdate err",
			zap.Int("datasetId", datasetId),
			zap.Any("updateData", updateData),
			zap.Error(err),
		)
		return err
	}

	_, err = db.GetSqlx().Exec(ctx, cond, vals...)
	if err != nil {
		log.Error(ctx, "UpdateOne fail.", zap.Int("datasetId", datasetId), zap.Any("updateData", updateData), zap.Error(err))
		return err
	}
	return nil
}

// 查询最近活跃的数据集
func QueryActivateList(ctx context.Context, activateTime time.Time, limit uint) ([]*Model, error) {
	where := map[string]interface{}{
		"activate_time >": activateTime,
		"status in": []byte{
			byte(pb.Status_Status_Running),
			byte(pb.Status_Status_Stopping),
			byte(pb.Status_Status_Deleting),
		},
		"_orderby": "activate_time asc",
		"_limit":   []uint{0, limit},
	}
	cond, vals, err := builder.BuildSelect(tableName, where, []string{"dataset_id", "activate_time", "status"})
	if err != nil {
		log.Error(ctx, "QueryActivateList BuildSelect err",
			zap.Any("where", where),
			zap.Error(err),
		)
		return nil, err
	}

	ret := []*Model{}
	err = db.GetSqlx().Find(ctx, &ret, cond, vals...)
	if err != nil {
		log.Error(ctx, "QueryActivateList Find fail.", zap.String("cond", cond), zap.Any("vals", vals), zap.Error(err))
		return nil, err
	}
	return ret, nil
}
