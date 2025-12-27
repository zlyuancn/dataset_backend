package module

import (
	"context"
	"strconv"

	"github.com/bytedance/sonic"
	"github.com/spf13/cast"
	"github.com/zly-app/cache/v2"
	"github.com/zly-app/component/redis"
	"github.com/zly-app/component/sqlx"
	"github.com/zly-app/zapp/log"
	"github.com/zly-app/zapp/pkg/utils"
	"go.uber.org/zap"

	"github.com/zlyuancn/dataset/client/db"
	"github.com/zlyuancn/dataset/conf"
	"github.com/zlyuancn/dataset/dao/dataset_list"
	"github.com/zlyuancn/dataset/model"
	"github.com/zlyuancn/dataset/pb"
)

var Dataset = &datasetCli{}

type datasetCli struct{}

// 写入或删除停止标记
func (*datasetCli) SetStopFlag(ctx context.Context, datasetId int, flag model.StopFlag) error {
	key := CacheKey.GetStopFlag(datasetId)
	rdb, err := db.GetRedis()
	if err != nil {
		return err
	}
	switch flag {
	case model.StopFlag_None:
		err = rdb.Del(ctx, key).Err()
	case model.StopFlag_Stop:
		err = rdb.SetNX(ctx, key, strconv.Itoa(int(model.StopFlag_Stop)), 0).Err()
	}
	return err
}

// 获取停止标记
func (*datasetCli) GetStopFlag(ctx context.Context, datasetId int) (model.StopFlag, error) {
	key := CacheKey.GetStopFlag(datasetId)
	rdb, err := db.GetRedis()
	if err != nil {
		return model.StopFlag_None, err
	}
	v, err := rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return model.StopFlag_None, nil
	}
	if err != nil {
		log.Error(ctx, "GetStopFlag fail.", zap.Error(err))
		return model.StopFlag_None, err
	}
	return model.StopFlag(cast.ToInt(v)), nil
}

// 获取数据集信息, 使用缓存
func (*datasetCli) GetDatasetInfoByCache(ctx context.Context, datasetId uint) (*dataset_list.Model, error) {
	key := CacheKey.GetDatasetInfo(int(datasetId))
	ret := &dataset_list.Model{}

	// 单纯的从cache获取数据
	err := cache.GetDefCache().Get(ctx, key, ret)
	if err != nil && err != cache.ErrCacheMiss {
		log.Error(ctx, "GetDatasetInfoByCache fail.", zap.Error(err))
		return nil, err
	}

	// 从db加载数据
	err = cache.GetDefCache().SingleFlightDo(ctx, key, ret, cache.WithLoadFn(func(ctx context.Context, key string) (interface{}, error) {
		v, err := dataset_list.GetOneByDatasetId(ctx, int(datasetId))
		if err == sqlx.ErrNoRows {
			return nil, nil
		}
		return v, err
	}))
	if err == cache.ErrDataIsNil {
		er := cache.GetDefCache().Set(ctx, key, nil, cache.WithExpire(conf.Conf.DatasetNotFinishedInfoCacheTtl))
		if er != nil {
			log.Error(ctx, "GetDatasetInfoByCache call Set fail.", zap.Error(err))
		}
		return nil, err
	}
	if err != nil {
		log.Error(ctx, "GetDatasetInfoByCache call SingleFlightDo fail.", zap.Error(err))
		return nil, err
	}

	// 写入缓存
	expire := conf.Conf.DatasetInfoCacheTtl
	if ret.Status != byte(pb.Status_Status_Finished) {
		expire = conf.Conf.DatasetNotFinishedInfoCacheTtl
	}
	err = cache.GetDefCache().Set(ctx, key, ret, cache.WithExpire(expire))
	if err != nil {
		log.Error(ctx, "GetDatasetInfoByCache call Set fail.", zap.Error(err))
	}
	return ret, err
}

// 批量获取数据集信息, 使用缓存
func (d *datasetCli) BatchGetDatasetInfoByCache(ctx context.Context, datasetId []uint) ([]*dataset_list.Model, error) {
	// 批量获取数据
	lines, err := utils.GoQuery(datasetId, func(id uint) (*dataset_list.Model, error) {
		line, err := d.GetDatasetInfoByCache(ctx, id)
		if err != nil {
			log.Error(ctx, "BatchGetDatasetInfoByCache call GetDatasetInfoByCache fail.", zap.Uint("id", id), zap.Error(err))
			return nil, err
		}
		return line, nil
	}, true)
	if err != nil {
		log.Error(ctx, "BatchGetDatasetInfoByCache call query fail.", zap.Error(err))
		return nil, err
	}
	return lines, nil
}

func (d *datasetCli) LoadCacheProcessStatus(ctx context.Context, datasetId int) (*model.CacheDatasetProcessStatus, bool, error) {
	key := CacheKey.GetCacheDatasetProcessStatus(datasetId)
	rdb, err := db.GetRedis()
	if err != nil {
		return nil, false, err
	}
	v, err := rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	ret := &model.CacheDatasetProcessStatus{}
	err = sonic.UnmarshalString(v, ret)
	if err != nil {
		log.Error(ctx, "LoadCacheProcessStatus UnmarshalString fail.", zap.String("v", v), zap.Error(err))
		return nil, false, err
	}

	return ret, true, nil
}
