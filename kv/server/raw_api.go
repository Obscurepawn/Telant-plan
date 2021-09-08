package server

import (
	"context"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (resp *kvrpcpb.RawGetResponse, err error) {
	// Your Code Here (1).

	resp = &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(nil)
	if err != nil {
		log.Error("[Server.RawGet] get reader failed:%v", err.Error())
		resp.Error = err.Error()
		return
	}

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		log.Error("[Server.RawGet] get reader failed:%v", err.Error())
		resp.Error = err.Error()
		if err == badger.ErrKeyNotFound {
			err = nil
			resp.NotFound = true
		}
	}
	resp.Value = val
	return
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (resp *kvrpcpb.RawPutResponse, err error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	resp = &kvrpcpb.RawPutResponse{}

	batch := []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}
	log.Info("[server.RawPut] rawput batch:%v", batch)
	if err = server.storage.Write(nil, batch); err != nil {
		resp.Error = err.Error()
		return
	}

	return
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (resp *kvrpcpb.RawDeleteResponse, err error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	resp = &kvrpcpb.RawDeleteResponse{}

	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	}

	if err = server.storage.Write(nil, batch); err != nil {
		resp.Error = err.Error()
		return
	}
	return
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (resp *kvrpcpb.RawScanResponse, err error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	resp = &kvrpcpb.RawScanResponse{}

	reader, err := server.storage.Reader(nil)
	if err != nil {
		resp.Error = err.Error()
		return
	}

	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)

	kvpairs := []*kvrpcpb.KvPair{}

	for i := 0; i < int(req.Limit); i++ {
		val, err := iter.Item().Value()
		if err != nil {
			resp.Error = err.Error()
			return resp, err
		}
		kvpairs = append(kvpairs, &kvrpcpb.KvPair{Key: iter.Item().Key(), Value: val})
	}

	resp.Kvs = kvpairs
	return
}
