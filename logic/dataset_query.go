package logic

import (
	"context"

	"github.com/bytedance/sonic"
	"github.com/zly-app/cache/v2"
	"github.com/zly-app/zapp/log"
	"github.com/zlyuancn/dataset/dao/dataset"
	"github.com/zlyuancn/dataset/module"
	"github.com/zlyuancn/dataset/pb"
	"go.uber.org/zap"
)

func (*Dataset) SearchDataset(ctx context.Context, req *pb.SearchDatasetReq) (*pb.SearchDatasetRsp, error) {
	where := map[string]any{}
	switch {
	case req.GetDatasetName() != "": // 仅搜索数据集名
		where["dataset_name"] = req.GetDatasetName()
	case req.GetOpUser() != "": // 仅搜索用户名
		where["_or"] = []map[string]interface{}{
			{
				"op_user_id like": req.GetOpUser() + "%",
			},
			{
				"op_user_name like": req.GetOpUser() + "%",
			},
		}
	default:
		return &pb.SearchDatasetRsp{}, nil
	}

	pageSize := max(req.GetPageSize(), 5)
	where["_limit"] = []uint{0, uint(pageSize)}
	where["_orderby"] = "dataset_id desc"

	lines, err := dataset.MultiGetBySelect(ctx, where, []string{"dataset_id", "dataset_name"})
	if err != nil {
		log.Error(ctx, "SearchDataset call dataset.MultiGetBySelect", zap.Error(err))
		return nil, err
	}

	data := make([]*pb.SearchDatasetLine, len(lines))
	for i := range lines {
		data[i] = &pb.SearchDatasetLine{
			DatasetId:   int64(lines[i].DatasetId),
			DatasetName: lines[i].DatasetName,
		}
	}
	return &pb.SearchDatasetRsp{Dataset: data}, nil
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

		// todo 对于运行中的任务, 进度需要从redis获取

		return &pb.QueryDatasetListRsp{Lines: ret}, nil
	}

	where := map[string]any{
		"dataset_id <": req.GetNextCursor(),
	}
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

	pageSize := max(req.GetPageSize(), 5)
	where["_limit"] = []uint{0, uint(pageSize)}
	where["_orderby"] = "dataset_id desc"

	// 获取id列表
	ids, err := dataset.MultiGetId(ctx, where)
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

	// todo 对于运行中的任务, 进度需要从redis获取

	nextCursor := int64(0)
	if len(ret) > 0 {
		nextCursor = ret[len(ret)-1].DatasetId
	}
	return &pb.QueryDatasetListRsp{
		NextCursor: nextCursor,
		Lines:      ret,
	}, nil
}

func (d *Dataset) QueryDatasetInfo(ctx context.Context, req *pb.QueryDatasetInfoReq) (*pb.QueryDatasetInfoRsp, error) {
	line, err := module.Dataset.GetDatasetInfoByCache(ctx, uint(req.GetDatasetId()))
	if err != nil {
		log.Error(ctx, "QueryDatasetInfo call GetDatasetInfoByCache fail.", zap.Error(err))
		return nil, err
	}
	ret := d.datasetDbModel2Pb(line)

	// todo 对于运行中的任务, 进度需要从redis获取

	return &pb.QueryDatasetInfoRsp{Line: ret}, nil
}

func (d *Dataset) datasetDbModel2Pb(line *dataset.Model) *pb.DatasetInfoA {
	de := &pb.DatasetExtend{}
	if line.DatasetExtend != "" {
		_ = sonic.UnmarshalString(line.DatasetExtend, de)
	}
	ret := &pb.DatasetInfoA{
		DatasetId:     int64(line.DatasetId),
		DatasetName:   line.DatasetName,
		Remark:        line.Remark,
		DatasetExtend: de,
		ValueTotal:    int32(line.ValueTotal),
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

func (d *Dataset) datasetDBModel2ListPb(line *dataset.Model) *pb.DatasetInfoByListA {
	de := &pb.DatasetExtend{}
	if line.DatasetExtend != "" {
		_ = sonic.UnmarshalString(line.DatasetExtend, de)
	}
	ret := &pb.DatasetInfoByListA{
		DatasetId:   int64(line.DatasetId),
		DatasetName: line.DatasetName,
		Remark:      line.Remark,
		ValueTotal:  int32(line.ValueTotal),
		CreateTime:  line.CreateTime.Unix(),
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

func (d *Dataset) QueryDatasetStatusInfo(ctx context.Context, req *pb.QueryDatasetStatusInfoReq) (*pb.QueryDatasetStatusInfoRsp, error) {
	return &pb.QueryDatasetStatusInfoRsp{}, nil
}
func (d *Dataset) QueryDatasetData(ctx context.Context, req *pb.QueryDatasetDataReq) (*pb.QueryDatasetDataRsp, error) {
	return &pb.QueryDatasetDataRsp{}, nil
}
