package module

import (
	"strconv"

	"github.com/zlyuancn/dataset_backend/conf"
)

var CacheKey = cacheKeyCli{}

type cacheKeyCli struct{}

// 数据集信息缓存key
func (cacheKeyCli) GetDatasetInfo(datasetId int) string {
	return conf.Conf.DatasetInfoKeyPrefix + strconv.Itoa(datasetId)
}

// 停止标记key
func (*cacheKeyCli) GetStopFlag(datasetId int) string {
	return conf.Conf.StopProcessFlagPrefix + strconv.Itoa(datasetId)
}

// 缓存的数据集处理状态
func (*cacheKeyCli) GetCacheDatasetProcessStatus(datasetId int) string {
	return conf.Conf.DatasetProcessStatusPrefix + strconv.Itoa(datasetId)
}

func (*cacheKeyCli) GetChunkMeta(datasetId int) string {
	return conf.Conf.ChunkMetaKeyPrefix + strconv.Itoa(datasetId)
}

// 获取运行处理锁
func (*cacheKeyCli) GetRunProcessLock(datasetId int) string {
	return conf.Conf.RunLockPrefix + strconv.Itoa(datasetId)
}
