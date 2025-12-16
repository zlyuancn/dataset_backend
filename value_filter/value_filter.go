package value_filter

import (
	"github.com/zlyuancn/dataset/pb"
	"github.com/zlyuancn/splitter"
)

func NewValueFilter(vp *pb.ValueProcess) splitter.ValueFilter {
	return func(value []byte) []byte {
		return value
	}
}
