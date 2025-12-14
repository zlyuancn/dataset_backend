package conf

import (
	"github.com/zly-app/zapp/config"
	"github.com/zly-app/zapp/log"
)

const ConfigKey = "dataset"

const (
	defSqlxName  = "dataset"
	defRedisName = "dataset"

	defDatasetOpLockKeyPrefix = "dataset:op_lock:"
	defAdminOpLockTtl         = 10
	defStopProcessFlagPrefix  = "batch_job:stop:"
	defRunProcessLockPrefix   = "batch_job:run:"
	defRunProcessLockExtraTtl = 600
)

var Conf = Config{
	SqlxName:  defSqlxName,
	RedisName: defRedisName,

	DatasetOpLockKeyPrefix: defDatasetOpLockKeyPrefix,
	AdminOpLockTtl:         defAdminOpLockTtl,
	StopProcessFlagPrefix:  defStopProcessFlagPrefix,
	RunProcessLockPrefix:   defRunProcessLockPrefix,
	RunProcessLockExtraTtl: defRunProcessLockExtraTtl,
}

type Config struct {
	// 组件名

	SqlxName  string // sqlx组件名
	RedisName string // redis组件名

	// redisKey

	DatasetOpLockKeyPrefix string // 数据集操作锁前缀
	AdminOpLockTtl         int    // admin 接口操作锁 ttl , 单位秒
	StopProcessFlagPrefix  string // 数据集停止处理flag
	RunProcessLockPrefix   string // 运行处理锁前缀
	RunProcessLockExtraTtl int    // 任务处理运行锁的ttl, 单位秒, 用于防止重复运行启动器
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
	if conf.RunProcessLockPrefix == "" {
		conf.RunProcessLockPrefix = defRunProcessLockPrefix
	}
	if conf.RunProcessLockExtraTtl < 1 {
		conf.RunProcessLockExtraTtl = defRunProcessLockExtraTtl
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
