package standalone_storage

import (
	"path"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	conf *config.Config
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := path.Join(conf.DBPath, "kv") 
	raftPath := path.Join(conf.DBPath, "raft")
	
	kvEngine := engine_util.CreateDB(kvPath, false)
	var raftEngine *badger.DB
	if conf.Raft {
		raftEngine = engine_util.CreateDB(raftPath, true)
	}
	engine := engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath)
	
	return &StandAloneStorage{
		conf: conf,
		engine: engine,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.engine.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engine.Kv.NewTransaction(false)
	reader := NewStandAloneStorageReader(txn)
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var err error
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			err = engine_util.PutCF(s.engine.Kv, modify.Cf(), modify.Key(), modify.Value())
		case storage.Delete:
			err = engine_util.DeleteCF(s.engine.Kv, modify.Cf(), modify.Key())
		}
	}
	return err
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorageReader(t *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{
		txn: t,
	}
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err:= engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}
