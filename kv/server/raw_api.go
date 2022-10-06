package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.Cf, req.Key)
	var rsp kvrpcpb.RawGetResponse
	if val == nil {
		rsp.NotFound = true
	}
	rsp.Value = val

	return &rsp, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var put = storage.Put{
		Cf:    req.Cf,
		Key:   req.Key,
		Value: req.Value,
	}
	var batch = []storage.Modify{
		{
			Data: put,
		},
	}
	err := server.storage.Write(nil, batch)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var delete = storage.Delete{
		Cf:  req.Cf,
		Key: req.Key,
	}

	var batch = []storage.Modify{
		{
			Data: delete,
		},
	}
	err := server.storage.Write(nil, batch)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	iter.Seek(req.StartKey)
	var i uint32
	i = 0 
	var kvs []*kvrpcpb.KvPair
	
	for i < req.Limit && iter.Valid() {
		item := iter.Item()
		key := item.Key()
		value, _ := item.Value()
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key: key,
			Value: value,
		})
		iter.Next()
		i++
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, err
}
