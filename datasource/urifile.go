package datasource

import (
	"context"
	"io"
	"strings"

	"github.com/zly-app/component/http"
	"github.com/zly-app/zapp/filter"
	"github.com/zlyuancn/dataset/pb"
)

type uriFileDataSource struct {
	ctx    context.Context
	uf     *pb.DataSourceUriFile
	closer io.Closer
}

func (u *uriFileDataSource) SetBreakpoint(offset int64) (bool, error) {
	// 暂不支持断点续传
	return false, nil
}

func (u *uriFileDataSource) GetDataStreamLen() int64 {
	return 0
}

func (u *uriFileDataSource) GetReader() (io.Reader, error) {
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

	ctx := filter.WithoutFilterName(u.ctx, "base.timeout", "base.gpool")
	sp, err := http.NewClient("uriFileDataSource").Do(ctx, r)
	if err != nil {
		return nil, err
	}
	u.closer = sp.BodyStream
	return sp.BodyStream, nil
}

func (u *uriFileDataSource) Close() error {
	return u.closer.Close()
}

func newUriFileDataSource(ctx context.Context, dp *pb.DataProcess) (DataSource, error) {
	u := &uriFileDataSource{
		ctx: ctx,
		uf:  dp.GetUriFile(),
	}
	return u, nil
}
