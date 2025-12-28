package module

import (
	"context"
	"time"

	"github.com/zly-app/cache/v2"
	"github.com/zly-app/zapp/log"
	"github.com/zlyuancn/redis_tool"
	"go.uber.org/zap"

	"github.com/zlyuancn/dataset_backend/conf"
	"github.com/zlyuancn/dataset_backend/dao/dataset_list"
	"github.com/zlyuancn/dataset_backend/handler"
	"github.com/zlyuancn/dataset_backend/model"
	"github.com/zlyuancn/dataset_backend/pb"
)

// 尝试停止
func (processorCli) RestorerStop(ctx context.Context, datasetId int) {
	// 获取运行锁
	lockKey := CacheKey.GetRunProcessLock(datasetId)
	unlock, _, err := redis_tool.AutoLock(ctx, lockKey, time.Duration(conf.Conf.RunLockExtraTtl)*time.Second)
	if err == redis_tool.LockIsUsedByAnother { // 加锁失败
		return
	}
	if err != nil { // 加锁异常
		log.Error("RestorerStop call AutoLock fail.", zap.Error(err))
		return
	}
	defer unlock()

	// 更新为已停止
	t := time.Now()
	updateData := map[string]any{
		"status": int(pb.Status_Status_Stopped),
	}
	err = dataset_list.UpdateOne(ctx, datasetId, updateData, 0)
	if err != nil {
		log.Error(ctx, "RestorerStop call UpdateOne fail.", zap.Error(err))
		return
	}

	// 清除数据缓存
	err = cache.GetDefCache().Del(ctx, CacheKey.GetDatasetInfo(datasetId))
	if err != nil {
		log.Error(ctx, "RestorerStop call clear Cache fail.", zap.Error(err))
		// return err
	}

	// 删除停止flag
	err = Dataset.SetStopFlag(ctx, datasetId, model.StopFlag_None)
	if err != nil {
		log.Error(ctx, "RestorerStop call SetStopFlag None fail.", zap.Error(err))
	}

	// handler
	datasetInfo, err := Dataset.GetDatasetInfoByCache(ctx, uint(datasetId))
	if err != nil {
		log.Error(ctx, "RestorerStop call GetDatasetInfoByCache fail.", zap.Error(err))
	} else {
		datasetInfo.Status = byte(pb.Status_Status_Stopped)
		handler.Trigger(ctx, handler.AfterDatasetProcessStopped, &handler.Info{
			T:       t,
			Dataset: datasetInfo,
		})
	}
}
