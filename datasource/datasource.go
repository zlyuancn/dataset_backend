package datasource

import (
	"context"
	"fmt"
	"io"

	"github.com/zlyuancn/dataset/pb"
)

type DataSource interface {
	// 设置断点, 如果设置成功则返回true, 设置失败表示从头开始读取
	SetBreakpoint(offset int64) (bool, error)
	// 尝试获取数据流长度
	GetDataStreamLen() int64
	// 获取读取器
	GetReader() (io.Reader, error)
	// 关闭
	Close() error
}

type creatorFunc func(ctx context.Context, dp *pb.DataProcess) (DataSource, error)

var creator = map[pb.DataSource]creatorFunc{
	pb.DataSource_DataSource_UriFile: newUriFileDataSource,
}

func NewDataSource(ctx context.Context, dp *pb.DataProcess) (DataSource, error) {
	c, ok := creator[dp.GetDataSource()]
	if !ok {
		return nil, fmt.Errorf("not support DataSource type=%d", dp.GetDataSource())
	}
	return c(ctx, dp)
}
