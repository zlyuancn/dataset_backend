package logic

import (
	"github.com/zlyuancn/dataset_backend/pb"
)

type Dataset struct {
	pb.UnimplementedDatasetServiceServer
}

func NewServer() pb.DatasetServiceServer {
	return &Dataset{}
}
