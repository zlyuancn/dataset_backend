package module

import (
	"context"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zlyuancn/dataset_backend/pb"
)

const target = "localhost:3000"
const method = "/dataset.DatasetService/QueryDatasetData"
const datasetId = 20
const maxValueSn = 9999

func DatasetQuery(b *testing.B, client pb.DatasetServiceClient, valueSn int64) {
	sp, err := client.QueryDatasetData(context.Background(), &pb.QueryDatasetDataReq{DatasetId: datasetId, ValueSn: valueSn})
	if err != nil {
		b.Fatalf("http post err: %v", err)
	}
	if sp.ValueSn != valueSn {
		b.Fatalf("http post err: valueSn != valueSn")
	}
}

func BenchmarkDatasetQuery1(b *testing.B) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatalf("NewClient failed: %v", err)
	}
	client := pb.NewDatasetServiceClient(conn)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DatasetQuery(b, client, int64(i%(maxValueSn+1)))
	}
}

func BenchmarkDatasetQuery2(b *testing.B) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatalf("NewClient failed: %v", err)
	}
	client := pb.NewDatasetServiceClient(conn)

	b.ResetTimer()
	var i int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sn := atomic.AddInt64(&i, 1)
			DatasetQuery(b, client, sn%(maxValueSn+1))
		}
	})
}
