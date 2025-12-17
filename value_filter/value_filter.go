package value_filter

import (
	"github.com/zlyuancn/dataset/pb"
)

type ValueFilter interface {
	Handler(value []byte) []byte
}

type valueFilter struct {
	vp *pb.ValueProcess
}

func (v *valueFilter) Handler(value []byte) []byte {
	// todo 过滤
	return value
}

func NewValueFilter(vp *pb.ValueProcess) (ValueFilter, error) {
	vf := &valueFilter{
		vp: vp,
	}
	return vf, nil
}
