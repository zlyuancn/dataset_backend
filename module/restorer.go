package module

import (
	"context"
	"time"

	"github.com/zly-app/zapp"
	"github.com/zly-app/zapp/component/gpool"
	"github.com/zly-app/zapp/core"
	"github.com/zly-app/zapp/log"
	"github.com/zly-app/zapp/pkg/utils"
	"go.uber.org/zap"

	"github.com/zlyuancn/dataset_backend/conf"
	"github.com/zlyuancn/dataset_backend/dao/dataset_list"
	"github.com/zlyuancn/dataset_backend/model"
	"github.com/zlyuancn/dataset_backend/pb"
)

// 查询一次活跃任务limit数量
const oneQueryActivateJobLimit = 30

var Restorer = newRestorer()

type restorerCli struct {
}

func newRestorer() *restorerCli {
	return &restorerCli{}
}

func (r *restorerCli) Start() {
	go r.start()
}
func (r *restorerCli) start() {
	interval := time.Duration(conf.Conf.RecoverIntervalTimeSec) * time.Second
	t := time.NewTicker(interval)

	zapp.AddHandler(zapp.BeforeCloseService, func(app core.IApp, handlerType zapp.HandlerType) {
		t.Stop()
	})

	for range t.C {
		ctx := utils.Trace.CtxStart(context.Background(), "Restorer.Start")
		utils.Trace.CtxEnd(ctx)

		err := r.restorer(ctx)
		if err != nil {
			log.Error(ctx, "restorer fail.", zap.Error(err))
		}
	}
}

func (r *restorerCli) restorer(ctx context.Context) error {
	nextQueryTime := time.Now().Add(-time.Duration(conf.Conf.RecoverProcessLastActivateDay) * time.Hour * 24)
	for {
		// 查询最近创建的数据集
		lines, err := dataset_list.QueryActivateList(ctx, nextQueryTime, oneQueryActivateJobLimit)
		if err != nil {
			log.Error(ctx, "Restorer call dataset_list.QueryActivateList fail.", zap.Error(err))
			return err
		}

		// 扫描完成
		if len(lines) == 0 {
			return nil
		}

		// 恢复任务, 并设置下一个查询时间
		nextQueryTime, err = r.restorerProcess(ctx, lines)
		if err != nil {
			log.Error(ctx, "Restorer call restorerProcess fail.", zap.Error(err))
			return err
		}
	}
}

// 恢复任务
func (r *restorerCli) restorerProcess(ctx context.Context, lines []*dataset_list.Model) (time.Time, error) {
	nextQueryTime := time.Now()
	for i, line := range lines {
		// 重设活动时间
		if !line.ActivateTime.IsZero() && (i == 0 || nextQueryTime.Before(line.ActivateTime)) {
			nextQueryTime = line.ActivateTime
		}

		// 对于停止中的状态处理
		if pb.Status(line.Status) == pb.Status_Status_Stopping {
			cloneCtx := utils.Ctx.CloneContext(ctx)
			datasetId := line.DatasetId
			gpool.GetDefGPool().Go(func() error {
				Processor.RestorerStop(cloneCtx, int(datasetId))
				return nil
			}, nil)
			continue
		}

		// 对于删除中的状态处理
		if pb.Status(line.Status) == pb.Status_Status_Deleting {
			// todo 恢复删除过程
			continue
		}

		// ----- 以下为运行中的状态处理 -----

		// 获取停止标记
		stopFlag, err := Dataset.GetStopFlag(ctx, int(line.DatasetId))
		if err != nil {
			log.Error(ctx, "restorerProcess call GetStopFlag fail.", zap.Uint("datasetId", line.DatasetId), zap.Error(err))
		}
		if stopFlag == model.StopFlag_Stop {
			cloneCtx := utils.Ctx.CloneContext(ctx)
			datasetId := line.DatasetId
			gpool.GetDefGPool().Go(func() error {
				Processor.RestorerStop(cloneCtx, int(datasetId))
				return nil
			}, nil)
			continue
		}

		// 恢复处理
		cloneCtx := utils.Ctx.CloneContext(ctx)
		datasetId := line.DatasetId
		gpool.GetDefGPool().Go(func() error {
			Processor.CreateProcessor(cloneCtx, int(datasetId), true)
			return nil
		}, nil)
	}
	return nextQueryTime, nil
}
