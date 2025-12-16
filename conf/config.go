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
	defDatasetInfoKeyPrefix              = "dataset:info:"
	defDatasetInfoCacheTtl               = 3600
	defDatasetProcessStatusPrefix        = "dataset:status:"
)

var Conf = Config{
	SqlxName:  defSqlxName,
	RedisName: defRedisName,

	DatasetOpLockKeyPrefix:            defDatasetOpLockKeyPrefix,
	AdminOpLockTtl:                    defAdminOpLockTtl,
	StopProcessFlagPrefix:             defStopProcessFlagPrefix,
	CheckStopFlagInterval:             defCheckStopFlagInterval,
	RunLockPrefix:                     defRunLockPrefix,
	RunLockExtraTtl:                   defRunLockExtraTtl,
	RunLockRenewInterval:              defRunLockRenewInterval,
	RunLockRenewMaxContinuousErrCount: defRunLockRenewMaxContinuousErrCount,
	DatasetInfoKeyPrefix:              defDatasetInfoKeyPrefix,
	DatasetInfoCacheTtl:               defDatasetInfoCacheTtl,
	DatasetProcessStatusPrefix:        defDatasetProcessStatusPrefix,
}

type Config struct {
	// 组件名

	SqlxName  string // sqlx组件名
	RedisName string // redis组件名

	// redisKey

	DatasetOpLockKeyPrefix            string // 数据集操作锁前缀
	AdminOpLockTtl                    int    // admin 接口操作锁 ttl , 单位秒
	StopProcessFlagPrefix             string // 数据集停止处理flag
	CheckStopFlagInterval             int    // 检查停止标记间隔
	RunLockPrefix                     string // 运行处理锁前缀
	RunLockExtraTtl                   int    // 任务处理运行锁的ttl, 单位秒, 用于防止重复运行启动器
	RunLockRenewInterval              int    // 任务处理运行锁续期间隔秒数
	RunLockRenewMaxContinuousErrCount int    // 续期最大连续错误次数
	DatasetInfoKeyPrefix              string // 数据集信息缓存前缀
	DatasetInfoCacheTtl               int    // 数据集信息缓存ttl秒数
	DatasetProcessStatusPrefix        string // 数据集处理状态缓存前缀
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
		conf.CheckStopFlagInterval = 1
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
	if conf.DatasetInfoKeyPrefix == "" {
		conf.DatasetInfoKeyPrefix = defDatasetInfoKeyPrefix
	}
	if conf.DatasetInfoCacheTtl < 1 {
		conf.DatasetInfoCacheTtl = defDatasetInfoCacheTtl
	}
	if conf.DatasetProcessStatusPrefix == "" {
		conf.DatasetProcessStatusPrefix = defDatasetProcessStatusPrefix
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
