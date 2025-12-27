package logic

import (
	"context"
	"errors"

	"github.com/bytedance/sonic"
	"github.com/spf13/cast"
	"github.com/zly-app/cache/v2"
	"github.com/zly-app/component/redis"
	"github.com/zly-app/zapp/log"
	"go.uber.org/zap"

	"github.com/zlyuancn/dataset/client/db"
	"github.com/zlyuancn/dataset/dao/dataset_list"
	"github.com/zlyuancn/dataset/model"
	"github.com/zlyuancn/dataset/module"
	"github.com/zlyuancn/dataset/pb"
)

func (*Dataset) SearchDatasetName(ctx context.Context, req *pb.SearchDatasetNameReq) (*pb.SearchDatasetNameRsp, error) {
	where := map[string]any{}
	switch {
	case req.GetDatasetName() != "": // 仅搜索数据集名
		where["dataset_name"] = req.GetDatasetName()
	case req.GetDatasetId() > 0: // 直接根据数据集id搜索
		d, err := module.Dataset.GetDatasetInfoByCache(ctx, uint(req.GetDatasetId()))
		if err != nil {
			log.Error(ctx, "SearchDatasetName call GetDatasetInfoByCache fail.", zap.Error(err))
			return nil, err
		}
		data := []*pb.SearchDatasetNameLine{
			{
				DatasetId:   int64(d.DatasetId),
				DatasetName: d.DatasetName,
			},
		}
		return &pb.SearchDatasetNameRsp{Lines: data}, nil
	default:
		return &pb.SearchDatasetNameRsp{}, nil
	}

	pageSize := max(req.GetPageSize(), 5)
	where["_limit"] = []uint{0, uint(pageSize)}
	where["_orderby"] = "dataset_id desc"

	lines, err := dataset_list.MultiGetBySelect(ctx, where, []string{"dataset_id", "dataset_name"})
	if err != nil {
		log.Error(ctx, "SearchDatasetName call dataset.MultiGetBySelect", zap.Error(err))
		return nil, err
	}

	data := make([]*pb.SearchDatasetNameLine, len(lines))
	for i := range lines {
		data[i] = &pb.SearchDatasetNameLine{
			DatasetId:   int64(lines[i].DatasetId),
			DatasetName: lines[i].DatasetName,
		}
	}
	return &pb.SearchDatasetNameRsp{Lines: data}, nil
}

func (d *Dataset) QueryDatasetList(ctx context.Context, req *pb.QueryDatasetListReq) (*pb.QueryDatasetListRsp, error) {
	// 直接查询id
	if req.GetDatasetId() != 0 {
		line, err := module.Dataset.GetDatasetInfoByCache(ctx, uint(req.GetDatasetId()))
		if err == cache.ErrDataIsNil {
			return &pb.QueryDatasetListRsp{}, nil
		}
		if err != nil {
			log.Error(ctx, "QueryDatasetList call GetDatasetInfoByCache fail.", zap.Error(err))
			return nil, err
		}
		ret := []*pb.DatasetInfoByListA{d.datasetDBModel2ListPb(line)}
		return &pb.QueryDatasetListRsp{Lines: ret}, nil
	}

	where := map[string]any{}
	if len(req.GetStatus()) > 0 {
		raw := req.GetStatus()
		status := make([]int, len(raw))
		for i := range raw {
			status[i] = int(raw[i])
		}
		where["status in"] = status
	}
	if req.GetStartTime() > 0 {
		where["create_time >="] = req.GetStartTime()
	}
	if req.GetEndTime() > 0 {
		where["create_time <="] = req.GetEndTime()
	}
	if req.GetOpUser() != "" {
		where["_or"] = []map[string]interface{}{
			{
				"op_user_id like": req.GetOpUser() + "%",
			},
			{
				"op_user_name like": req.GetOpUser() + "%",
			},
		}
	}

	page, pageSize := max(req.GetPage(), 1), max(req.GetPageSize(), 10)

	var total int64
	if page == 1 {
		t, err := dataset_list.Count(ctx, where)
		if err != nil {
			log.Error(ctx, "QueryDatasetList call dataset_list.Count", zap.Error(err))
			return nil, err
		}
		total = t
	}

	where["_orderby"] = "dataset_id desc"
	where["_limit"] = []uint{uint(page-1) * uint(pageSize), uint(pageSize)}

	// 获取id列表
	ids, err := dataset_list.MultiGetId(ctx, where)
	if err != nil {
		log.Error(ctx, "QueryDatasetList call dataset.MultiGetId", zap.Error(err))
		return nil, err
	}

	// 批量获取数据
	lines, err := module.Dataset.BatchGetDatasetInfoByCache(ctx, ids)
	if err != nil {
		log.Error(ctx, "QueryDatasetList call BatchGetDatasetInfoByCache fail.", zap.Error(err))
		return nil, err
	}

	// 数据转换
	ret := make([]*pb.DatasetInfoByListA, 0, len(lines))
	for _, line := range lines {
		ret = append(ret, d.datasetDBModel2ListPb(line))
	}

	return &pb.QueryDatasetListRsp{
		Total: int32(total),
		Lines: ret,
	}, nil
}

func (d *Dataset) QueryDatasetInfo(ctx context.Context, req *pb.QueryDatasetInfoReq) (*pb.QueryDatasetInfoRsp, error) {
	line, err := module.Dataset.GetDatasetInfoByCache(ctx, uint(req.GetDatasetId()))
	if err != nil {
		log.Error(ctx, "QueryDatasetInfo call GetDatasetInfoByCache fail.", zap.Error(err))
		return nil, err
	}
	ret := d.datasetDbModel2Pb(line)
	return &pb.QueryDatasetInfoRsp{Line: ret}, nil
}

func (d *Dataset) datasetDbModel2Pb(line *dataset_list.Model) *pb.DatasetInfoA {
	de := &pb.DatasetExtend{}
	if line.DatasetExtend != "" {
		_ = sonic.UnmarshalString(line.DatasetExtend, de)
	}
	ret := &pb.DatasetInfoA{
		DatasetId:     int64(line.DatasetId),
		DatasetName:   line.DatasetName,
		Remark:        line.Remark,
		DatasetExtend: de,
		ValueTotal:    line.ValueTotal,
		CreateTime:    line.CreateTime.Unix(),
		ProcessedTime: line.ProcessedTime,
		Op: &pb.OpInfoA{
			OpSource:   line.OpSource,
			OpUserid:   line.OpUserId,
			OpUserName: line.OpUserName,
			OpRemark:   line.OpRemark,
			OpTime:     line.UpdateTime.Unix(),
		},
		Status:       pb.Status(line.Status),
		StatusInfo:   line.StatusInfo,
		ActivateTime: line.ActivateTime.Unix(),
	}
	return ret
}

func (d *Dataset) datasetDBModel2ListPb(line *dataset_list.Model) *pb.DatasetInfoByListA {
	de := &pb.DatasetExtend{}
	if line.DatasetExtend != "" {
		_ = sonic.UnmarshalString(line.DatasetExtend, de)
	}
	ret := &pb.DatasetInfoByListA{
		DatasetId:     int64(line.DatasetId),
		DatasetName:   line.DatasetName,
		Remark:        line.Remark,
		Delim:         de.ValueProcess.Delim,
		ValueTotal:    line.ValueTotal,
		CreateTime:    line.CreateTime.Unix(),
		ProcessedTime: line.ProcessedTime,
		Op: &pb.OpInfoA{
			OpSource:   line.OpSource,
			OpUserid:   line.OpUserId,
			OpUserName: line.OpUserName,
			OpRemark:   line.OpRemark,
			OpTime:     line.UpdateTime.Unix(),
		},
		Status:     pb.Status(line.Status),
		StatusInfo: line.StatusInfo,
	}
	return ret
}

func (d *Dataset) datasetDBModel2StatusPb(line *dataset_list.Model) *pb.DatasetStateInfo {
	de := &pb.DatasetExtend{}
	if line.DatasetExtend != "" {
		_ = sonic.UnmarshalString(line.DatasetExtend, de)
	}
	ret := &pb.DatasetStateInfo{
		DatasetId:           int64(line.DatasetId),
		ChunkTotal:          0,
		ChunkProcessedCount: 0,
		ValueTotal:          line.ValueTotal,
		Status:              pb.Status(line.Status),
		StatusInfo:          line.StatusInfo,
		ProcessedTime:       line.ProcessedTime,
		Op: &pb.OpInfoA{
			OpSource:   line.OpSource,
			OpUserid:   line.OpUserId,
			OpUserName: line.OpUserName,
			OpRemark:   line.OpRemark,
			OpTime:     line.UpdateTime.Unix(),
		},
	}
	return ret
}

func (d *Dataset) QueryDatasetStatusInfo(ctx context.Context, req *pb.QueryDatasetStatusInfoReq) (*pb.QueryDatasetStatusInfoRsp, error) {
	// 批量获取数据
	ids := make([]uint, len(req.GetDatasetIds()))
	for i, id := range req.GetDatasetIds() {
		ids[i] = uint(id)
	}
	lines, err := module.Dataset.BatchGetDatasetInfoByCache(ctx, ids)
	if err != nil {
		log.Error(ctx, "QueryDatasetStatusInfo call BatchGetDatasetInfoByCache fail.", zap.Error(err))
		return nil, err
	}

	// 数据转换
	ret := make([]*pb.DatasetStateInfo, 0, len(lines))
	for _, line := range lines {
		ret = append(ret, d.datasetDBModel2StatusPb(line))
	}

	// 对于运行中的任务, 进度需要从redis获取
	_ = d.batchRenderRunningProcess(ctx, ret)

	return &pb.QueryDatasetStatusInfoRsp{DatasetStateInfos: ret}, nil
}

// 批量渲染运行中任务进度
func (*Dataset) batchRenderRunningProcess(ctx context.Context, ret []*pb.DatasetStateInfo) error {
	lines := make([]*pb.DatasetStateInfo, 0, len(ret))
	keys := make([]string, 0, len(lines))
	for _, l := range ret {
		lines = append(lines, l)
		keys = append(keys, module.CacheKey.GetCacheDatasetProcessStatus(int(l.DatasetId)))
	}
	if len(keys) == 0 {
		return nil
	}

	rdb, err := db.GetRedis()
	if err != nil {
		return err
	}

	// 查询
	values, err := rdb.MGet(ctx, keys...).Result()
	if err != nil && err != redis.Nil {
		log.Error(ctx, "batchRenderRunningProcess call mget fail.", zap.Error(err))
		return err
	}
	if len(values) != len(keys) {
		err = errors.New("reply nums not match keys")
		log.Error(ctx, "batchRenderRunningProcess call mget fail.", zap.Error(err))
		return err
	}

	for i, line := range lines {
		if values[i] != nil {
			s := &model.CacheDatasetProcessStatus{}
			_ = sonic.UnmarshalString(cast.ToString(values[i]), s)
			line.ChunkTotal = s.ChunkTotal
			line.ChunkProcessedCount = s.ChunkFinishedCount
			line.ValueTotal = s.ValueFinishedCount
		}
	}
	return nil
}

func (d *Dataset) QueryDatasetData(ctx context.Context, req *pb.QueryDatasetDataReq) (*pb.QueryDatasetDataRsp, error) {
	chunkSn, value, err := module.Query.GetValue(ctx, req.GetDatasetId(), req.GetValueSn())
	if err != nil {
		log.Error(ctx, "QueryDatasetData call GetValue fail.", zap.Error(err))
		return nil, err
	}

	return &pb.QueryDatasetDataRsp{
		DatasetId: req.GetDatasetId(),
		ChunkSn:   chunkSn,
		ValueSn:   req.GetValueSn(),
		Value:     value,
	}, nil
}
