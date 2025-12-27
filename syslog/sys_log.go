package syslog

import (
	"context"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/zly-app/zapp"
	"github.com/zly-app/zapp/core"
	"github.com/zly-app/zapp/log"
	"github.com/zlyuancn/rotate"
	"github.com/zlyuancn/zretry"

	"github.com/zlyuancn/dataset_backend/conf"
	"github.com/zlyuancn/dataset_backend/dao/dataset_log"
	"github.com/zlyuancn/dataset_backend/handler"
	"github.com/zlyuancn/dataset_backend/pb"
)

var sl *sysLog

type sysLog struct {
	r     rotate.IRotator[*dataset_log.Model]
	level byte
	write bool
}

func Init() {
	sl = &sysLog{}

	if !conf.Conf.SysLogWriteDatabase {
		return
	}

	sl.write = true

	c := &rotate.RotateConf{
		BatchSize:       conf.Conf.SysLogBatchSize,
		AutoRotateTime:  time.Duration(conf.Conf.SysLogAutoRotateTimeSec) * time.Second,
		CloseAutoRotate: conf.Conf.SysLogCloseAutoRotate,
	}
	sl.r = rotate.NewRotateWithConf(c, sl.flush)

	// app退出前主动轮转
	zapp.AddHandler(zapp.AfterCloseService, func(app core.IApp, handlerType zapp.HandlerType) {
		sl.r.Rotate()
	})

	// 等级限制
	level := byte(pb.DataLogType_DataLogType_Info)
	switch strings.ToLower(conf.Conf.SysLogWriteLevel) {
	case "info":
		level = byte(pb.DataLogType_DataLogType_Info)
	case "warn":
		level = byte(pb.DataLogType_DataLogType_Warn)
	case "err":
		level = byte(pb.DataLogType_DataLogType_Err)
	}
	sl.level = level

	handler.AddHandler(handler.AfterCreateDataset, sl.jobHandler)
	handler.AddHandler(handler.AfterUpdateDataset, sl.jobHandler)
	handler.AddHandler(handler.AfterDeleteDataset, sl.jobHandler)
	handler.AddHandler(handler.AfterRunDatasetProcess, sl.jobHandler)
	handler.AddHandler(handler.AfterDatasetProcessStopped, sl.jobHandler)
	handler.AddHandler(handler.DatasetProcessRunFailureExit, sl.jobHandler)
	handler.AddHandler(handler.DatasetProcessRestorer, sl.jobHandler)
	handler.AddHandler(handler.DatasetProcessFinished, sl.jobHandler)

}

func (*sysLog) flush(values []*dataset_log.Model) {
	_ = zretry.DoRetry(
		conf.Conf.SysLogFlushAttemptCount,
		time.Duration(conf.Conf.SysLogFlushErrIntervalSec)*time.Second,
		func() error {
			_, err := dataset_log.MultiSave(context.Background(), values)
			return err
		}, func(nowAttemptCount, remainCount int, err error) {
			if remainCount == 0 {
				log.Warn("sysLog write fail. wait retry. nowAttemptCount=%d err:", nowAttemptCount, err)
			} else {
				log.Error("sysLog write fail. nowAttemptCount=%d err:", nowAttemptCount, err)
			}
		},
	)
}

func (j *sysLog) jobHandler(ctx context.Context, handlerType handler.HandlerType, info *handler.Info) {
	switch handlerType {
	case handler.AfterCreateDataset:
		extend, _ := sonic.MarshalString(info.Dataset)
		j.Log(&dataset_log.Model{
			DatasetId:  uint64(info.Dataset.DatasetId),
			Remark:     "create dataset",
			Extend:     extend,
			LogType:    byte(pb.DataLogType_DataLogType_Info),
			CreateTime: info.T.UnixMicro(),
		})
	case handler.AfterUpdateDataset:
		extend, _ := sonic.MarshalString(info.Dataset)
		j.Log(&dataset_log.Model{
			DatasetId:  uint64(info.Dataset.DatasetId),
			Remark:     "update dataset",
			Extend:     extend,
			LogType:    byte(pb.DataLogType_DataLogType_Info),
			CreateTime: info.T.UnixMicro(),
		})
	case handler.AfterDeleteDataset:
		j.Log(&dataset_log.Model{
			DatasetId:  uint64(info.Dataset.DatasetId),
			Remark:     "delete dataset",
			LogType:    byte(pb.DataLogType_DataLogType_Warn),
			CreateTime: info.T.UnixMicro(),
		})
	case handler.AfterRunDatasetProcess:
		j.Log(&dataset_log.Model{
			DatasetId:  uint64(info.Dataset.DatasetId),
			Remark:     "run dataset",
			LogType:    byte(pb.DataLogType_DataLogType_Info),
			CreateTime: info.T.UnixMicro(),
		})
	case handler.AfterDatasetProcessStopped:
		j.Log(&dataset_log.Model{
			DatasetId:  uint64(info.Dataset.DatasetId),
			Remark:     "stop dataset",
			LogType:    byte(pb.DataLogType_DataLogType_Info),
			CreateTime: info.T.UnixMicro(),
		})
	case handler.DatasetProcessRunFailureExit:
		j.Log(&dataset_log.Model{
			DatasetId:  uint64(info.Dataset.DatasetId),
			Remark:     "run dataset failure exit",
			Extend:     info.Dataset.StatusInfo,
			LogType:    byte(pb.DataLogType_DataLogType_Err),
			CreateTime: info.T.UnixMicro(),
		})
	case handler.DatasetProcessRestorer:
		j.Log(&dataset_log.Model{
			DatasetId:  uint64(info.Dataset.DatasetId),
			Remark:     "restorer dataset",
			LogType:    byte(pb.DataLogType_DataLogType_Info),
			CreateTime: info.T.UnixMicro(),
		})
	case handler.DatasetProcessFinished:
		j.Log(&dataset_log.Model{
			DatasetId:  uint64(info.Dataset.DatasetId),
			Remark:     "dataset finished",
			LogType:    byte(pb.DataLogType_DataLogType_Info),
			CreateTime: info.T.UnixMicro(),
		})
	}
}

func (j *sysLog) Log(v *dataset_log.Model) {
	if !j.write || v.LogType < j.level {
		return
	}
	j.r.Add(v)
}

func Log(v *dataset_log.Model) {
	sl.Log(v)
}
