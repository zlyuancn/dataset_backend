package datasource

import (
	"context"
	"io"
	"strings"

	"github.com/spf13/cast"
	"github.com/zly-app/component/http"
	"github.com/zly-app/zapp/filter"
	"github.com/zly-app/zapp/log"
	"go.uber.org/zap"

	"github.com/zlyuancn/dataset/pb"
)

type uriFileDataSource struct {
	ctx    context.Context
	uf     *pb.DataSourceUriFile
	closer io.Closer
}

func (u *uriFileDataSource) SetBreakpoint(ctx context.Context, offset int64) (bool, error) {
	// 暂不支持断点续传
	return false, nil
}

// 尝试数据长度
func (u *uriFileDataSource) GetDataStreamLen(ctx context.Context) int64 {
	header := make(http.Header, len(u.uf.GetHeaders()))
	for _, kv := range u.uf.GetHeaders() {
		header.Set(kv.GetK(), kv.GetV())
	}
	r := &http.Request{
		Method:             "HEAD",
		Path:               u.uf.GetUri(),
		InsecureSkipVerify: u.uf.GetInsecureSkipVerify(),
		Header:             header,
		OutIsStream:        true, // 响应是一个流
		Proxy:              u.uf.GetProxy(),
	}

	ctx = filter.WithoutFilterName(ctx, "base.gpool")
	sp, err := http.NewClient("uriFileDataSource").Do(ctx, r)
	if err != nil {
		log.Error(ctx, "GetDataStreamLen fail.", zap.Error(err))
		return 0
	}
	_ = sp.BodyStream.Close()
	return cast.ToInt64(sp.Header.Get("Content-Length"))
}

func (u *uriFileDataSource) GetReader(ctx context.Context) (io.Reader, error) {
	method := strings.ToUpper(u.uf.GetMethod())
	if method == "" {
		method = "GET"
	}
	header := make(http.Header, len(u.uf.GetHeaders()))
	for _, kv := range u.uf.GetHeaders() {
		header.Set(kv.GetK(), kv.GetV())
	}
	r := &http.Request{
		Method:             method,
		Path:               u.uf.GetUri(),
		InsecureSkipVerify: u.uf.GetInsecureSkipVerify(),
		Header:             header,
		OutIsStream:        true, // 响应是一个流
		Proxy:              u.uf.GetProxy(),
	}

	ctx = filter.WithoutFilterName(ctx, "base.timeout", "base.gpool")
	sp, err := http.NewClient("uriFileDataSource").Do(ctx, r)
	if err != nil {
		return nil, err
	}
	u.closer = sp.BodyStream
	return sp.BodyStream, nil
}

func (u *uriFileDataSource) Close() {
	_ = u.closer.Close()
	return
}

func newUriFileDataSource(ctx context.Context, dp *pb.DataProcess) (DataSource, error) {
	u := &uriFileDataSource{
		uf: dp.GetUriFile(),
	}
	return u, nil
}
