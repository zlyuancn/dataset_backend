package conf

import (
	"github.com/zly-app/zapp/config"
	"github.com/zly-app/zapp/log"
)

const ConfigKey = "dataset"

const (
	defSqlxName  = "dataset"
	defRedisName = "dataset"

	defDatasetOpLockKeyPrefix            = "dataset:op_lock:"
	defAdminOpLockTtl                    = 10
	defStopProcessFlagPrefix             = "dataset:stop_flag:"
	defCheckStopFlagInterval             = 1
	defRunLockPrefix                     = "dataset:run_lock:"
	defRunLockExtraTtl                   = 600
	defRunLockRenewInterval              = 120
	defRunLockRenewMaxContinuousErrCount = 3
	defDatasetProcessStatusPrefix        = "dataset:status:"
	defChunkMetaKeyPrefix                = "dataset:chunk_meta:"

	defDatasetInfoKeyPrefix           = "dataset:info:"
	defDatasetInfoCacheTtl            = 3600
	defDatasetNotFinishedInfoCacheTtl = 300

	defChunkStoreRedisName                   = "dataset"
	defChunkStoreRedisKeyFormat              = "dataset:chunk_store:%d_%d"
	defChunkSizeLimit                        = 8 * 1024 * 1024
	defMinChunkSizeLimit                     = 1 * 1024
	defValueMaxScanSizeLimit                 = 1 * 1024 * 1024
	defChunkStoreThreadCount                 = 5
	defChunkMetaLruCacheCount                = 100
	defChunkDataLruCacheCount                = 100
	defChunkCompressedRatioLimit             = 85
	defChunkPreloadByValueExpendRatio        = 90
	defChunkPreloadProbabilityWithValueCount = 100
)

var Conf = Config{
	SqlxName:  defSqlxName,
	RedisName: defRedisName,

	// redis Key
	DatasetOpLockKeyPrefix:            defDatasetOpLockKeyPrefix,
	AdminOpLockTtl:                    defAdminOpLockTtl,
	StopProcessFlagPrefix:             defStopProcessFlagPrefix,
	CheckStopFlagInterval:             defCheckStopFlagInterval,
	RunLockPrefix:                     defRunLockPrefix,
	RunLockExtraTtl:                   defRunLockExtraTtl,
	RunLockRenewInterval:              defRunLockRenewInterval,
	RunLockRenewMaxContinuousErrCount: defRunLockRenewMaxContinuousErrCount,
	DatasetProcessStatusPrefix:        defDatasetProcessStatusPrefix,
	ChunkMetaKeyPrefix:                defChunkMetaKeyPrefix,

	// cache Key

	DatasetInfoKeyPrefix:           defDatasetInfoKeyPrefix,
	DatasetInfoCacheTtl:            defDatasetInfoCacheTtl,
	DatasetNotFinishedInfoCacheTtl: defDatasetNotFinishedInfoCacheTtl,

	// chunk

	ChunkStoreRedisName:                   defChunkStoreRedisName,
	ChunkStoreRedisKeyFormat:              defChunkStoreRedisKeyFormat,
	ChunkSizeLimit:                        defChunkSizeLimit,
	ValueMaxScanSizeLimit:                 defValueMaxScanSizeLimit,
	ChunkStoreThreadCount:                 defChunkStoreThreadCount,
	ChunkMetaLruCacheCount:                defChunkMetaLruCacheCount,
	ChunkDataLruCacheCount:                defChunkDataLruCacheCount,
	ChunkCompressedRatioLimit:             defChunkCompressedRatioLimit,
	ChunkPreloadByValueExpendRatio:        defChunkPreloadByValueExpendRatio,
	ChunkPreloadProbabilityWithValueCount: defChunkPreloadProbabilityWithValueCount,
}

type Config struct {
	// 组件名

	SqlxName  string // sqlx组件名
	RedisName string // redis组件名

	// redis

	DatasetOpLockKeyPrefix            string // 数据集操作锁前缀
	AdminOpLockTtl                    int    // admin 接口操作锁 ttl , 单位秒
	StopProcessFlagPrefix             string // 数据集停止处理flag
	CheckStopFlagInterval             int    // 检查停止标记间隔
	RunLockPrefix                     string // 运行处理锁前缀
	RunLockExtraTtl                   int    // 任务处理运行锁的ttl, 单位秒, 用于防止重复运行启动器
	RunLockRenewInterval              int    // 任务处理运行锁续期间隔秒数
	RunLockRenewMaxContinuousErrCount int    // 续期最大连续错误次数
	DatasetProcessStatusPrefix        string // 数据集处理状态缓存前缀
	ChunkMetaKeyPrefix                string // chunk meta key 前缀

	// cache Key

	DatasetInfoKeyPrefix           string // 数据集信息缓存前缀
	DatasetInfoCacheTtl            int    // 数据集信息缓存ttl秒数
	DatasetNotFinishedInfoCacheTtl int    // 数据集未处理完成的数据缓存ttl

	// chunk

	ChunkStoreRedisName                   string // chunk store redis 组件名
	ChunkStoreRedisKeyFormat              string // chunk store redis key格式
	ChunkSizeLimit                        int32  // chunk 大小限制
	ValueMaxScanSizeLimit                 int    // value 扫描最大长度限制
	ChunkStoreThreadCount                 int    // chunk 持久化线程数
	ChunkMetaLruCacheCount                int    // chunk meta 的 lru 缓存最大条数
	ChunkDataLruCacheCount                int    // chunk 数据的 lru 缓存最大条数
	ChunkCompressedRatioLimit             int    // 压缩后的数据为原始数据的多少百分比才会视为有意义的压缩. 0表示不检查
	ChunkPreloadByValueExpendRatio        int    // 当 valueSn 在当前 chunk 的百分比位置之后触发预加载下一个 chunk. 0表示不预加载
	ChunkPreloadProbabilityWithValueCount int    // 当 valueSn 在预加载阈值时并不是直接进行预加载, 而是有概率的, 这个值表示会有多少个 value 会命中该概率阈值, 0 表示不计算概率直接进行预加载(有性能损耗)
}

func (conf *Config) Check() {
	if conf.SqlxName == "" {
		conf.SqlxName = defSqlxName
	}
	if conf.RedisName == "" {
		conf.RedisName = defRedisName
	}

	if conf.DatasetOpLockKeyPrefix == "" {
		conf.DatasetOpLockKeyPrefix = defDatasetOpLockKeyPrefix
	}
	if conf.AdminOpLockTtl < 1 {
		conf.AdminOpLockTtl = defAdminOpLockTtl
	}
	if conf.StopProcessFlagPrefix == "" {
		conf.StopProcessFlagPrefix = defStopProcessFlagPrefix
	}
	if conf.CheckStopFlagInterval < 1 {
		conf.CheckStopFlagInterval = defCheckStopFlagInterval
	}
	if conf.RunLockPrefix == "" {
		conf.RunLockPrefix = defRunLockPrefix
	}
	if conf.RunLockExtraTtl < 1 {
		conf.RunLockExtraTtl = defRunLockExtraTtl
	}
	if conf.RunLockRenewInterval < 1 {
		conf.RunLockRenewInterval = defRunLockRenewInterval
	}
	if conf.RunLockRenewMaxContinuousErrCount < 1 {
		conf.RunLockRenewMaxContinuousErrCount = defRunLockRenewMaxContinuousErrCount
	}
	if conf.DatasetProcessStatusPrefix == "" {
		conf.DatasetProcessStatusPrefix = defDatasetProcessStatusPrefix
	}
	if conf.ChunkMetaKeyPrefix == "" {
		conf.ChunkMetaKeyPrefix = defChunkMetaKeyPrefix
	}

	if conf.DatasetInfoKeyPrefix == "" {
		conf.DatasetInfoKeyPrefix = defDatasetInfoKeyPrefix
	}
	if conf.DatasetInfoCacheTtl < 1 {
		conf.DatasetInfoCacheTtl = defDatasetInfoCacheTtl
	}
	if conf.DatasetNotFinishedInfoCacheTtl < 1 {
		conf.DatasetNotFinishedInfoCacheTtl = defDatasetNotFinishedInfoCacheTtl
	}

	if conf.ChunkStoreRedisName == "" {
		conf.ChunkStoreRedisName = defChunkStoreRedisName
	}
	if conf.ChunkStoreRedisKeyFormat == "" {
		conf.ChunkStoreRedisKeyFormat = defChunkStoreRedisKeyFormat
	}
	conf.ChunkSizeLimit = max(conf.ChunkSizeLimit, defMinChunkSizeLimit)
	if conf.ValueMaxScanSizeLimit < 1 {
		conf.ValueMaxScanSizeLimit = defValueMaxScanSizeLimit
	}
	if conf.ChunkStoreThreadCount < 1 {
		conf.ChunkStoreThreadCount = defChunkStoreThreadCount
	}
	if conf.ChunkDataLruCacheCount < 1 {
		conf.ChunkDataLruCacheCount = defChunkDataLruCacheCount
	}
}

func Init() error {
	err := config.Conf.Parse(ConfigKey, &Conf, true)
	if err != nil {
		log.Error("Parse config fail. err=", err)
		return err
	}
	Conf.Check()
	return nil
}
