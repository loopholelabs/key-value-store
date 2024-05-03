package libp2p

import (
	"context"
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

type DataStore struct {
	data map[datastore.Key][]byte // Store kv in here for now...
}

func NewTestDataStore() *DataStore {
	return &DataStore{
		data: make(map[datastore.Key][]byte, 0),
	}
}

// impl datastore.Batching

func (ds *DataStore) Close() error {
	return nil
}

func (ds *DataStore) Sync(ctx context.Context, prefix datastore.Key) error {
	return nil
}

func (ds *DataStore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	ds.data[key] = value
	fmt.Printf("Put %s -> %x\n", key, value)
	return nil
}

func (ds *DataStore) Delete(ctx context.Context, key datastore.Key) error {
	delete(ds.data, key)
	fmt.Printf("Delete %s\n", key)
	return nil
}

func (ds *DataStore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	value, ok := ds.data[key]
	if !ok {
		fmt.Printf("Get %s\n", key)
		return nil, datastore.ErrNotFound
	}
	fmt.Printf("Get %s -> %x\n", key, value)
	return value, nil
}

func (ds *DataStore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	_, ok := ds.data[key]
	fmt.Printf("Has %s -> %t\n", key, ok)
	return ok, nil
}

func (ds *DataStore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	value, ok := ds.data[key]
	if !ok {
		fmt.Printf("GetSize %s\n", key)
		return 0, datastore.ErrNotFound
	}
	fmt.Printf("GetSize %s -> %d\n", key, len(value))
	return len(value), nil
}

// Hrmmm queries
func (ds *DataStore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	fmt.Printf("Query\n")
	return nil, datastore.ErrNotFound
}

// Support batching in a simple way
func (ds *DataStore) Batch(ctx context.Context) (datastore.Batch, error) {
	return datastore.NewBasicBatch(ds), nil
}
