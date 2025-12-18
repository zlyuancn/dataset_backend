package module

import (
	"context"
	"time"

	"github.com/zly-app/cache/v2"
	"github.com/zly-app/zapp/log"
	"github.com/zlyuancn/dataset/conf"
	"github.com/zlyuancn/dataset/dao/dataset_list"
	"github.com/zlyuancn/dataset/handler"
	"github.com/zlyuancn/dataset/pb"
	"github.com/zlyuancn/redis_tool"
	"go.uber.org/zap"
)

// 尝试停止
func (processorCli) RestorerStop(ctx context.Context, datasetInfo *dataset_list.Model) {
	// 获取运行锁
	lockKey := CacheKey.GetRunProcessLock(int(datasetInfo.DatasetId))
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
	updateData := map[string]any{
		"status": int(pb.Status_Status_Stopped),
	}
	err = dataset_list.UpdateOne(ctx, int(datasetInfo.DatasetId), updateData, 0)
	if err != nil {
		log.Error(ctx, "RestorerStop call UpdateOne fail.", zap.Error(err))
		return
	}

	handler.Trigger(ctx, handler.AfterDatasetProcessStopped, &handler.Info{
		Dataset: datasetInfo,
	})

	// 清除缓存
	err = cache.GetDefCache().Del(ctx, CacheKey.GetDatasetInfo(int(datasetInfo.DatasetId)))
	if err != nil {
		log.Error(ctx, "RestorerStop call clear Cache fail.", zap.Error(err))
		// return err
	}
}
