package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).

	db *badger.DB
}

type StandAloneStorageReader struct {
	tx *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	return &StandAloneStorage{
		db: engine_util.CreateDB(conf.DBPath, false),
	}
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) (resp []byte, err error) {
	if resp, err = engine_util.GetCFFromTxn(s.tx, cf, key); err != nil {
		log.Error("[StandAloneStorageReader.GetCF] GetCFFromTxn failed:%v", err.Error())
	}
	return
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.tx)
}

func (s *StandAloneStorageReader) Close() {
	//s.tx.Discard may cause panic
	defer util.PanicPack("StandAloneStorageReader.Close")

	s.tx.Discard()
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	tx := s.db.NewTransaction(false)
	return &StandAloneStorageReader{
		tx: tx,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) (err error) {
	// Your Code Here (1).

	for _, modify := range batch {
		if put, ok := modify.Data.(storage.Put); ok {
			if err = engine_util.PutCF(s.db, put.Cf, put.Key, put.Value); err != nil {
				log.Error("[StandAloneStorage.Write] PutCF failed:%v Put:%v", err.Error(), put)
				return
			}
			continue
		}
		if del, ok := modify.Data.(storage.Delete); ok {
			if err = engine_util.DeleteCF(s.db, del.Cf, del.Key); err != nil {
				log.Error("[StandAloneStorage.Write] DeleteCF failed:%v Del:%v", err.Error(), del)
			}
			return
		}
	}
	return nil
}
