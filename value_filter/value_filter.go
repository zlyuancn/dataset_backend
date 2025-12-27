package value_filter

import (
	"bytes"

	"github.com/zlyuancn/dataset_backend/pb"
)

type ValueFilter interface {
	Handler(value []byte) []byte
}

type valueFilter struct {
	vp *pb.ValueProcess
}

func (v *valueFilter) Handler(value []byte) []byte {
	// 修剪
	for {
		n := 0
		ok := false

		value, ok = v.trimSpace(value)
		if ok {
			n++
		}

		value, ok = v.trimPrefix(value)
		if ok {
			n++
		}

		value, ok = v.trimSuffix(value)
		if ok {
			n++
		}

		if n == 0 { // 再也无法修剪数据了
			break
		}
	}

	// 过滤
	for _, t := range v.vp.GetFilterSubString() {
		if bytes.Contains(value, []byte(t)) {
			return nil
		}
	}
	for _, t := range v.vp.GetFilterPrefix() {
		if bytes.HasPrefix(value, []byte(t)) {
			return nil
		}
	}
	for _, t := range v.vp.GetFilterSuffix() {
		if bytes.HasSuffix(value, []byte(t)) {
			return nil
		}
	}

	return value
}

func (v *valueFilter) trimSpace(value []byte) ([]byte, bool) {
	if !v.vp.GetTrimSpace() {
		return value, false
	}

	oldLen := len(value)
	value = bytes.TrimSpace(value)
	return value, len(value) != oldLen
}

func (v *valueFilter) trimPrefix(value []byte) ([]byte, bool) {
	if len(v.vp.GetTrimPrefix()) == 0 {
		return value, false
	}

	oldLen := len(value)
	for _, t := range v.vp.GetTrimPrefix() {
		value = bytes.TrimPrefix(value, []byte(t))
	}
	return value, len(value) != oldLen
}

func (v *valueFilter) trimSuffix(value []byte) ([]byte, bool) {
	if len(v.vp.GetTrimSuffix()) == 0 {
		return value, false
	}

	oldLen := len(value)
	for _, t := range v.vp.GetTrimSuffix() {
		value = bytes.TrimSuffix(value, []byte(t))
	}
	return value, len(value) != oldLen
}

func NewValueFilter(vp *pb.ValueProcess) (ValueFilter, error) {
	vf := &valueFilter{
		vp: vp,
	}
	return vf, nil
}
