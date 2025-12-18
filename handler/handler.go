package handler

import (
	"context"
	"time"

	"github.com/zly-app/zapp/component/gpool"
	"github.com/zly-app/zapp/pkg/utils"

	"github.com/zlyuancn/dataset/dao/dataset_list"
)

type Info struct {
	T       time.Time           // 触发时间
	Dataset *dataset_list.Model // 数据集
}

type Handler func(ctx context.Context, handlerType HandlerType, info *Info)

var handlers = map[HandlerType][]Handler{}

type HandlerType int

const (
	// 创建数据集后
	AfterCreateDataset HandlerType = iota

	// 更新数据集后
	AfterUpdateDataset

	// 删除数据集后
	AfterDeleteDataset

	// 启动数据集处理后
	AfterRunDatasetProcess

	// 数据集处理停止后
	AfterDatasetProcessStopped

	// 数据集处理故障退出
	DatasetProcessRunFailureExit
	// 数据集处理恢复运行
	DatasetProcessRestorer

	// 数据集处理完成后
	DatasetProcessFinished
)

// 添加handler
func AddHandler(t HandlerType, hs ...Handler) {
	handlers[t] = append(handlers[t], hs...)
}

// 触发
func Trigger(ctx context.Context, handlerType HandlerType, info *Info) {
	cloneCtx := utils.Ctx.CloneContext(ctx)
	if info.T.IsZero() {
		info.T = time.Now()
	}
	gpool.GetDefGPool().Go(func() error {
		for _, h := range handlers[handlerType] {
			h(cloneCtx, handlerType, info)
		}
		return nil
	}, nil)
}
