package logic

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/bytedance/sonic"
	"github.com/zly-app/cache/v2"
	"github.com/zly-app/zapp/component/gpool"
	"github.com/zly-app/zapp/log"
	"github.com/zly-app/zapp/pkg/utils"
	"github.com/zlyuancn/redis_tool"
	"go.uber.org/zap"

	"github.com/zlyuancn/dataset/module"

	"github.com/zlyuancn/dataset/conf"
	"github.com/zlyuancn/dataset/dao/dataset"
	"github.com/zlyuancn/dataset/dao/dataset_history"
	"github.com/zlyuancn/dataset/handler"
	"github.com/zlyuancn/dataset/model"
	"github.com/zlyuancn/dataset/pb"
)

func (*Dataset) AdminAddDataset(ctx context.Context, req *pb.AdminAddDatasetReq) (*pb.AdminAddDatasetRsp, error) {
	de, err := sonic.MarshalString(req.GetDatasetExtend())
	if err != nil {
		log.Error(ctx, "AdminAddDataset call MarshalString eed fail.", zap.Error(err))
		return nil, err
	}

	v := &dataset.Model{
		DatasetName:   req.GetDatasetName(),
		Remark:        req.GetRemark(),
		DatasetExtend: de,
		OpSource:      req.GetOp().GetOpSource(),
		OpUserId:      req.GetOp().GetOpUserid(),
		OpUserName:    req.GetOp().GetOpUserName(),
		OpRemark:      req.GetOp().GetOpRemark(),
		Status:        byte(pb.Status_Status_Created),
		StatusInfo:    model.StatusInfo_UserOp,
	}
	if req.GetStartProcessNow() {
		v.Status = byte(pb.Status_Status_Running)
		v.StatusInfo = model.StatusInfo_UserCreateAndRun
	}

	// 写入数据库
	datasetId, err := dataset.CreateOneModel(ctx, v)
	if err != nil {
		log.Error(ctx, "AdminAddDataset call CreateOneModel fail.", zap.Error(err))
		return nil, err
	}

	handler.Trigger(ctx, handler.AfterCreateDataset, &handler.Info{
		Dataset: v,
	})

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &dataset_history.Model{
		DatasetId:     uint(datasetId),
		DatasetName:   req.GetDatasetName(),
		Remark:        req.GetRemark(),
		DatasetExtend: de,
		OpSource:      req.GetOp().GetOpSource(),
		OpUserId:      req.GetOp().GetOpUserid(),
		OpUserName:    req.GetOp().GetOpUserName(),
		OpRemark:      req.GetOp().GetOpRemark(),
		OpCmd:         byte(pb.OpCmd_OpCmd_Create),
		Status:        byte(pb.Status_Status_Created),
		StatusInfo:    v.StatusInfo,
	}
	if req.GetStartProcessNow() {
		h.OpCmd = byte(pb.OpCmd_OpCmd_CreateAndRun)
	}
	gpool.GetDefGPool().Go(func() error {
		_, err := dataset_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			log.Error(cloneCtx, "AdminAddDataset call dataset_history.CreateOneModel fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	// 立即启动处理
	if req.GetStartProcessNow() {
		gpool.GetDefGPool().Go(func() error {
			module.Processor.CreateProcessor(cloneCtx, v)
			return nil
		}, nil)
	}

	return &pb.AdminAddDatasetRsp{DatasetId: datasetId}, nil
}

func (*Dataset) AdminUpdateDataset(ctx context.Context, req *pb.AdminUpdateDatasetReq) (*pb.AdminUpdateDatasetRsp, error) {
	de, err := sonic.MarshalString(req.GetDatasetExtend())
	if err != nil {
		log.Error(ctx, "AdminUpdateDataset call MarshalString eed fail.", zap.Error(err))
		return nil, err
	}

	// 加锁
	lockKey := conf.Conf.DatasetOpLockKeyPrefix + strconv.Itoa(int(req.GetDatasetId()))
	unlock, _, err := redis_tool.AutoLock(ctx, lockKey, time.Second*time.Duration(conf.Conf.AdminOpLockTtl))
	if err != nil {
		log.Error(ctx, "AdminUpdateDataset call AutoLock fail.", zap.Error(err))
		return nil, err
	}
	defer unlock()

	// 获取数据集
	d, err := dataset.GetOneByDatasetId(ctx, int(req.GetDatasetId()))
	if err != nil {
		log.Error(ctx, "AdminUpdateDataset call dataset.GetOneByDatasetId fail.", zap.Error(err))
		return nil, err
	}
	oldStatus := d.Status

	// 写入数据库的数据
	v := &dataset.Model{
		DatasetName: req.GetDatasetName(),
		Remark:      req.GetRemark(),
		OpSource:    req.GetOp().GetOpSource(),
		OpUserId:    req.GetOp().GetOpUserid(),
		OpUserName:  req.GetOp().GetOpUserName(),
		OpRemark:    req.GetOp().GetOpRemark(),
		StatusInfo:  model.StatusInfo_UserOp,
	}
	// 只有 created 状态才能更新 数据集扩展数据
	if d.Status == byte(pb.Status_Status_Created) {
		v.DatasetExtend = de
	}

	// 写入数据库
	_, err = dataset.AdminUpdateDataset(ctx, v, oldStatus)
	if err != nil {
		log.Error(ctx, "AdminUpdateDataset call dataset.AdminUpdateDataset fail.", zap.Error(err))
		return nil, err
	}

	d.DatasetName = req.GetDatasetName()
	d.Remark = req.GetRemark()
	d.OpSource = req.GetOp().GetOpSource()
	d.OpUserId = req.GetOp().GetOpUserid()
	d.OpUserName = req.GetOp().GetOpUserName()
	d.OpRemark = req.GetOp().GetOpRemark()
	d.StatusInfo = v.StatusInfo
	if d.Status == byte(pb.Status_Status_Created) {
		d.DatasetExtend = de
	}

	handler.Trigger(ctx, handler.AfterUpdateDataset, &handler.Info{
		Dataset: d,
	})

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &dataset_history.Model{
		DatasetId:     uint(req.GetDatasetId()),
		DatasetName:   req.GetDatasetName(),
		Remark:        req.GetRemark(),
		DatasetExtend: d.DatasetExtend,
		OpSource:      req.GetOp().GetOpSource(),
		OpUserId:      req.GetOp().GetOpUserid(),
		OpUserName:    req.GetOp().GetOpUserName(),
		OpRemark:      req.GetOp().GetOpRemark(),
		OpCmd:         byte(pb.OpCmd_OpCmd_Update),
		Status:        v.Status,
		StatusInfo:    v.StatusInfo,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = dataset_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			log.Error(cloneCtx, "AdminUpdateDataset call dataset_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetDatasetInfo(int(req.GetDatasetId())))
		if err != nil {
			log.Error(cloneCtx, "AdminUpdateDataset call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	return &pb.AdminUpdateDatasetRsp{}, nil
}

func (*Dataset) AdminDelDataset(ctx context.Context, req *pb.AdminDelDatasetReq) (*pb.AdminDelDatasetRsp, error) {
	// 加锁
	lockKey := conf.Conf.DatasetOpLockKeyPrefix + strconv.Itoa(int(req.GetDatasetId()))
	unlock, _, err := redis_tool.AutoLock(ctx, lockKey, time.Second*time.Duration(conf.Conf.AdminOpLockTtl))
	if err != nil {
		log.Error(ctx, "AdminDelDataset call AutoLock fail.", zap.Error(err))
		return nil, err
	}
	defer unlock()

	// 获取数据集
	d, err := dataset.GetOneByDatasetId(ctx, int(req.GetDatasetId()))
	if err != nil {
		log.Error(ctx, "AdminDelDataset call dataset.GetOneByDatasetId fail.", zap.Error(err))
		return nil, err
	}
	oldStatus := d.Status

	// 检查状态
	switch pb.Status(d.Status) {
	case pb.Status_Status_Created, pb.Status_Status_Finished, pb.Status_Status_Stopped:
	case pb.Status_Status_Running, pb.Status_Status_Stopping, pb.Status_Status_Finishing, pb.Status_Status_Deleting, pb.Status_Status_ChunkDeleted:
		log.Error(ctx, "AdminDelDataset fail. status is running", zap.Int64("dataset", req.GetDatasetId()))
		return nil, errors.New("Dataset status is running")
	default:
		log.Error(ctx, "AdminDelDataset fail. status is unknown", zap.Int64("dataset", req.GetDatasetId()))
		return nil, errors.New("Dataset status is unknown")
	}

	// 写入数据库的数据
	v := &dataset.Model{
		DatasetId:  uint(req.GetDatasetId()),
		Status:     byte(pb.Status_Status_Deleting),
		OpSource:   req.GetOp().GetOpSource(),
		OpUserId:   req.GetOp().GetOpUserid(),
		OpUserName: req.GetOp().GetOpUserName(),
		OpRemark:   req.GetOp().GetOpRemark(),
		StatusInfo: model.StatusInfo_UserOp,
	}

	// 更新数据为删除中
	count, err := dataset.AdminUpdateStatus(ctx, v, oldStatus)
	if err != nil {
		log.Error(ctx, "AdminDelDataset call dataset.AdminUpdateStatus fail.", zap.Error(err))
		return nil, err
	}
	if count != 1 {
		err = fmt.Errorf("update dataset status fail. update count != 1. is %d", count)
		log.Error(ctx, "AdminDelDataset call dataset.AdminUpdateStatus fail.", zap.Error(err))
		return nil, err
	}

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &dataset_history.Model{
		DatasetId:  uint(req.GetDatasetId()),
		OpSource:   req.GetOp().GetOpSource(),
		OpUserId:   req.GetOp().GetOpUserid(),
		OpUserName: req.GetOp().GetOpUserName(),
		OpRemark:   req.GetOp().GetOpRemark(),
		OpCmd:      byte(pb.OpCmd_OpCmd_Delete),
		Status:     v.Status,
		StatusInfo: model.StatusInfo_UserOp,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = dataset_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			log.Error(cloneCtx, "AdminDelDataset call dataset_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetDatasetInfo(int(req.GetDatasetId())))
		if err != nil {
			log.Error(cloneCtx, "AdminDelDataset call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	// todo 删除已持久化的chunk

	return &pb.AdminDelDatasetRsp{}, nil
}

func (*Dataset) AdminRunProcessDataset(ctx context.Context, req *pb.AdminRunProcessDatasetReq) (*pb.AdminRunProcessDatasetRsp, error) {
	// 加锁
	lockKey := conf.Conf.DatasetOpLockKeyPrefix + strconv.Itoa(int(req.GetDatasetId()))
	unlock, _, err := redis_tool.AutoLock(ctx, lockKey, time.Second*time.Duration(conf.Conf.AdminOpLockTtl))
	if err != nil {
		log.Error(ctx, "AdminRunProcessDataset call AutoLock fail.", zap.Error(err))
		return nil, err
	}
	defer unlock()

	// 获取数据集
	d, err := dataset.GetOneByDatasetId(ctx, int(req.GetDatasetId()))
	if err != nil {
		log.Error(ctx, "AdminRunProcessDataset call dataset.GetOneByDatasetId fail.", zap.Error(err))
		return nil, err
	}
	oldStatus := d.Status

	// 检查状态
	switch pb.Status(d.Status) {
	case pb.Status_Status_Running, pb.Status_Status_Stopping:
		log.Error(ctx, "AdminRunProcessDataset fail. status is running", zap.Int64("dataset", req.GetDatasetId()))
		return nil, errors.New("AdminRunProcessDataset status is running")
	case pb.Status_Status_Finishing, pb.Status_Status_Finished:
		log.Error(ctx, "AdminRunProcessDataset fail. status is finished", zap.Int64("dataset", req.GetDatasetId()))
		return nil, errors.New("AdminRunProcessDataset status is finished")
	case pb.Status_Status_Deleting, pb.Status_Status_ChunkDeleted:
		log.Error(ctx, "AdminRunProcessDataset fail. status is deleted", zap.Int64("dataset", req.GetDatasetId()))
		return nil, errors.New("Dataset status is deleted")
	case pb.Status_Status_Created, pb.Status_Status_Stopped:
	default:
		log.Error(ctx, "AdminRunProcessDataset fail. status is unknown", zap.Int64("dataset", req.GetDatasetId()))
		return nil, errors.New("Dataset status is unknown")
	}

	// 写入数据库的数据
	v := &dataset.Model{
		DatasetId:  uint(req.GetDatasetId()),
		Status:     byte(pb.Status_Status_Running),
		OpSource:   req.GetOp().GetOpSource(),
		OpUserId:   req.GetOp().GetOpUserid(),
		OpUserName: req.GetOp().GetOpUserName(),
		OpRemark:   req.GetOp().GetOpRemark(),
		StatusInfo: model.StatusInfo_UserChangeStatus,
	}
	d.Status = v.Status
	d.OpSource = req.GetOp().GetOpSource()
	d.OpUserId = req.GetOp().GetOpUserid()
	d.OpUserName = req.GetOp().GetOpUserName()
	d.OpRemark = req.GetOp().GetOpRemark()
	d.StatusInfo = v.StatusInfo

	// 更新数据为处理中
	count, err := dataset.AdminUpdateStatus(ctx, v, oldStatus)
	if err != nil {
		log.Error(ctx, "AdminRunProcessDataset call dataset.AdminUpdateStatus fail.", zap.Error(err))
		return nil, err
	}
	if count != 1 {
		err = fmt.Errorf("update dataset status fail. update count != 1. is %d", count)
		log.Error(ctx, "AdminRunProcessDataset call dataset.AdminUpdateStatus fail.", zap.Error(err))
		return nil, err
	}

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &dataset_history.Model{
		DatasetId:  uint(req.GetDatasetId()),
		OpSource:   req.GetOp().GetOpSource(),
		OpUserId:   req.GetOp().GetOpUserid(),
		OpUserName: req.GetOp().GetOpUserName(),
		OpRemark:   req.GetOp().GetOpRemark(),
		OpCmd:      byte(pb.OpCmd_OpCmd_Run),
		Status:     v.Status,
		StatusInfo: model.StatusInfo_UserChangeStatus,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = dataset_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			log.Error(cloneCtx, "AdminRunProcessDataset call dataset_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetDatasetInfo(int(req.GetDatasetId())))
		if err != nil {
			log.Error(cloneCtx, "AdminRunProcessDataset call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	// 启动
	gpool.GetDefGPool().Go(func() error {
		// 删除停止标记
		err = module.Dataset.SetStopFlag(cloneCtx, int(d.DatasetId), model.StopFlag_None)
		if err != nil {
			log.Error(cloneCtx, "AdminRunProcessDataset call DelStopFlag fail.", zap.Error(err))
			return err
		}

		// 开始处理
		module.Processor.CreateProcessor(ctx, d)
		return nil
	}, nil)

	return &pb.AdminRunProcessDatasetRsp{}, nil
}

func (*Dataset) AdminStopProcessDataset(ctx context.Context, req *pb.AdminStopProcessDatasetReq) (*pb.AdminStopProcessDatasetRsp, error) {
	// 加锁
	lockKey := conf.Conf.DatasetOpLockKeyPrefix + strconv.Itoa(int(req.GetDatasetId()))
	unlock, _, err := redis_tool.AutoLock(ctx, lockKey, time.Second*time.Duration(conf.Conf.AdminOpLockTtl))
	if err != nil {
		log.Error(ctx, "AdminStopProcessDataset call AutoLock fail.", zap.Error(err))
		return nil, err
	}
	defer unlock()

	// 获取数据集
	d, err := dataset.GetOneByDatasetId(ctx, int(req.GetDatasetId()))
	if err != nil {
		log.Error(ctx, "AdminStopProcessDataset call dataset.GetOneByDatasetId fail.", zap.Error(err))
		return nil, err
	}
	oldStatus := d.Status

	// 检查状态
	switch pb.Status(d.Status) {
	case pb.Status_Status_Running:
	case pb.Status_Status_Deleting, pb.Status_Status_ChunkDeleted:
		log.Error(ctx, "AdminStopProcessDataset fail. status is deleted", zap.Int64("dataset", req.GetDatasetId()))
		return nil, errors.New("Dataset status is deleted")
	case pb.Status_Status_Created, pb.Status_Status_Stopping, pb.Status_Status_Stopped, pb.Status_Status_Finishing, pb.Status_Status_Finished:
		log.Error(ctx, "AdminStopProcessDataset fail. status not running", zap.Int64("dataset", req.GetDatasetId()))
		return nil, errors.New("Dataset status is not running")
	default:
		log.Error(ctx, "AdminStopProcessDataset fail. status is unknown", zap.Int64("dataset", req.GetDatasetId()))
		return nil, errors.New("Dataset status is unknown")
	}

	// 写入停止标记
	err = module.Dataset.SetStopFlag(ctx, int(req.GetDatasetId()), model.StopFlag_Stop)
	if err != nil {
		log.Error(ctx, "AdminStopProcessDataset call SetStopFlag fail.", zap.Error(err))
		return nil, err
	}

	// 写入数据库的数据
	v := &dataset.Model{
		DatasetId:  uint(req.GetDatasetId()),
		Status:     byte(pb.Status_Status_Stopping),
		OpSource:   req.GetOp().GetOpSource(),
		OpUserId:   req.GetOp().GetOpUserid(),
		OpUserName: req.GetOp().GetOpUserName(),
		OpRemark:   req.GetOp().GetOpRemark(),
		StatusInfo: model.StatusInfo_UserChangeStatus,
	}

	// 更新数据为停止中
	count, err := dataset.AdminUpdateStatus(ctx, v, oldStatus)
	if err != nil {
		log.Error(ctx, "AdminStopProcessDataset call dataset.AdminUpdateStatus fail.", zap.Error(err))
		return nil, err
	}
	if count != 1 {
		err = fmt.Errorf("update dataset status fail. update count != 1. is %d", count)
		log.Error(ctx, "AdminStopProcessDataset call dataset.AdminUpdateStatus fail.", zap.Error(err))
		return nil, err
	}

	cloneCtx := utils.Ctx.CloneContext(ctx)
	// 添加历史记录
	h := &dataset_history.Model{
		DatasetId:  uint(req.GetDatasetId()),
		OpSource:   req.GetOp().GetOpSource(),
		OpUserId:   req.GetOp().GetOpUserid(),
		OpUserName: req.GetOp().GetOpUserName(),
		OpRemark:   req.GetOp().GetOpRemark(),
		OpCmd:      byte(pb.OpCmd_OpCmd_Stop),
		Status:     v.Status,
		StatusInfo: model.StatusInfo_UserChangeStatus,
	}
	gpool.GetDefGPool().Go(func() error {
		_, err = dataset_history.CreateOneModel(cloneCtx, h)
		if err != nil {
			log.Error(cloneCtx, "AdminStopProcessDataset call dataset_history.CreateOneModel fail.", zap.Error(err))
			// return nil, err
		}
		return nil
	}, nil)

	// 清除缓存
	gpool.GetDefGPool().Go(func() error {
		err := cache.GetDefCache().Del(cloneCtx, module.CacheKey.GetDatasetInfo(int(req.GetDatasetId())))
		if err != nil {
			log.Error(cloneCtx, "AdminStopProcessDataset call clear Cache fail.", zap.Error(err))
			// return err
		}
		return nil
	}, nil)

	// todo 尝试获取运行处理锁, 直接更新状态为已停止

	return &pb.AdminStopProcessDatasetRsp{}, nil
}
