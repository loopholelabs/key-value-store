/*
	Copyright 2023 Loophole Labs

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		   http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/

// Package badger implements the storage interface using badger
package badger

import (
	"context"
	"time"
	"unsafe"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/pb"
	kvstore "github.com/loopholelabs/kvstore/pkg/kvstore"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

var _ kvstore.KVStore = (*Badger)(nil)

type Options struct {
	StorageFile string
	LogName     string
}

type Badger struct {
	db     *badger.DB
	logger *zerolog.Logger
}

func New(storageFile string, logger *zerolog.Logger) (*Badger, error) {
	badgerOptions := badger.DefaultOptions(storageFile)
	if logger.Trace().Enabled() {
		traceLogger := logger.With().Str(zerolog.CallerFieldName, "BADGER").Logger()

		badgerOptions.Logger = &Logger{Logger: &traceLogger}
	} else {
		warnLogger := logger.With().Str(zerolog.CallerFieldName, "BADGER").Logger().Level(zerolog.WarnLevel)
		badgerOptions.Logger = &Logger{Logger: &warnLogger}
	}

	badgerOptions.Logger.Debugf("badger logger initialized")

	logger.Debug().Msgf("initializing badger with options: %+v", badgerOptions)
	database, err := badger.Open(badgerOptions)
	if err != nil {
		return nil, err
	}
	logger.Debug().Msg("badger started successfully")

	return &Badger{
		db:     database,
		logger: logger,
	}, nil
}

func (b *Badger) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	var value []byte
	var version uint64
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(unsafe.Slice(unsafe.StringData(key), len(key)))
		if err != nil {
			return err
		}
		version = item.Version()
		if item.ValueSize() == 0 {
			return nil
		}

		value, err = item.ValueCopy(nil)
		return err
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, 0, errors.Wrap(kvstore.ErrKeyNotFound, kvstore.ErrGetFailed.Error())
		}
		return nil, 0, errors.Wrap(err, kvstore.ErrGetFailed.Error())
	}

	return value, version, nil
}

func (b *Badger) GetFirst(ctx context.Context, prefix string) (*kvstore.Entry, error) {
	prefixBytes := unsafe.Slice(unsafe.StringData(prefix), len(prefix))
	entry := new(kvstore.Entry)
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(prefixBytes)
		if it.ValidForPrefix(prefixBytes) {
			var err error
			item := it.Item()
			entry.Version = item.Version()
			entry.Key = string(item.Key())
			if item.ValueSize() == 0 {
				return nil
			}
			value, err := item.ValueCopy(nil)
			if err == nil {
				entry.Value = value
			}
			return err
		} else {
			return kvstore.ErrKeyNotFound
		}
	})
	if err != nil {
		return nil, errors.Wrap(err, kvstore.ErrGetFirstFailed.Error())
	}

	return entry, nil
}

func (b *Badger) GetAll(ctx context.Context, prefix string) (kvstore.Entries, error) {
	var entries kvstore.Entries
	prefixBytes := unsafe.Slice(unsafe.StringData(prefix), len(prefix))
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			var entry kvstore.Entry
			item := it.Item()
			entry.Key = string(item.Key())
			entry.Version = item.Version()
			if item.ValueSize() != 0 {
				val, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				entry.Value = val
			}
			entries = append(entries, entry)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, kvstore.ErrGetAllFailed.Error())
	}

	return entries, nil
}

func (b *Badger) GetAllKeys(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	prefixBytes := unsafe.Slice(unsafe.StringData(prefix), len(prefix))
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			item := it.Item()
			keys = append(keys, string(item.Key()))
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, kvstore.ErrGetAllKeysFailed.Error())
	}

	return keys, nil
}

func (b *Badger) GetLimit(ctx context.Context, prefix string, limit int64) (kvstore.Entries, error) {
	var entries kvstore.Entries
	prefixBytes := unsafe.Slice(unsafe.StringData(prefix), len(prefix))
	err := b.db.View(func(txn *badger.Txn) error {
		var count int64 = 0
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			if count >= limit {
				break
			}
			var entry kvstore.Entry
			item := it.Item()
			entry.Key = string(item.Key())
			entry.Version = item.Version()
			if item.ValueSize() != 0 {
				val, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				entry.Value = val
			}
			entries = append(entries, entry)
			count++
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, kvstore.ErrGetAllFailed.Error())
	}

	return entries, nil
}

func (b *Badger) GetBatch(ctx context.Context, keys ...string) (kvstore.Entries, error) {
	var entries kvstore.Entries
	err := b.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			var entry kvstore.Entry
			item, err := txn.Get(unsafe.Slice(unsafe.StringData(key), len(key)))
			if err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue
				}
				return err
			}
			entry.Key = string(item.Key())
			entry.Version = item.Version()
			if item.ValueSize() != 0 {
				val, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				entry.Value = val
			}
			entries = append(entries, entry)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, kvstore.ErrGetBatchFailed.Error())
	}

	return entries, nil
}

func (b *Badger) Exists(ctx context.Context, key string) (bool, error) {
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(unsafe.Slice(unsafe.StringData(key), len(key)))
		return err
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return false, errors.Wrap(err, kvstore.ErrGetFailed.Error())
	}
	return true, nil
}

func (b *Badger) Set(ctx context.Context, key string, value []byte) error {
	// TODO: Use value directly instead of copying it
	//       once the issue is resolved:
	//       https://discuss.dgraph.io/t/reusing-byte-slice-in-transaction-causes-bugs-with-subscriptions/17204
	valueCopy := []byte(value)
	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(unsafe.Slice(unsafe.StringData(key), len(key)), valueCopy)
	})

	if err != nil {
		return errors.Wrap(err, kvstore.ErrSetFailed.Error())
	}

	return nil
}

func (b *Badger) SetEmpty(ctx context.Context, key string) error {
	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(unsafe.Slice(unsafe.StringData(key), len(key)), nil)
	})

	if err != nil {
		return errors.Wrap(err, kvstore.ErrSetEmptyFailed.Error())
	}

	return nil
}

func (b *Badger) SetIf(ctx context.Context, key string, value []byte, condition kvstore.Condition) error {
	// TODO: Use value directly instead of copying it
	//       once the issue is resolved:
	//       https://discuss.dgraph.io/t/reusing-byte-slice-in-transaction-causes-bugs-with-subscriptions/17204
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	keyBytes := unsafe.Slice(unsafe.StringData(key), len(key))
	err := b.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(keyBytes)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				if !condition(0) {
					return kvstore.ErrConditionFailed
				}
				return txn.Set(keyBytes, valueCopy)
			}
			return err
		}
		if !condition(item.Version()) {
			return kvstore.ErrConditionFailed
		}

		return txn.Set(keyBytes, valueCopy)
	})

	if err != nil {
		return errors.Wrap(err, kvstore.ErrSetIfFailed.Error())
	}

	return nil
}

func (b *Badger) SetDelete(ctx context.Context, key string, value []byte, delete string) error {
	// TODO: Use value directly instead of copying it
	//       once the issue is resolved:
	//       https://discuss.dgraph.io/t/reusing-byte-slice-in-transaction-causes-bugs-with-subscriptions/17204
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	err := b.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(unsafe.Slice(unsafe.StringData(key), len(key)), []byte(value))
		if err != nil {
			return err
		}
		return txn.Delete(unsafe.Slice(unsafe.StringData(delete), len(delete)))
	})

	if err != nil {
		return errors.Wrap(err, kvstore.ErrSetFailed.Error())
	}
	return nil
}

func (b *Badger) SetExpiry(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// TODO: Use value directly instead of copying it
	//       once the issue is resolved:
	//       https://discuss.dgraph.io/t/reusing-byte-slice-in-transaction-causes-bugs-with-subscriptions/17204
	valueCopy := []byte(value)
	err := b.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(unsafe.Slice(unsafe.StringData(key), len(key)), valueCopy).WithTTL(ttl)
		return txn.SetEntry(e)
	})

	if err != nil {
		return errors.Wrap(err, kvstore.ErrSetFailed.Error())
	}

	return nil
}

func (b *Badger) SetIfNotExist(ctx context.Context, key string, value []byte) error {
	// TODO: Use value directly instead of copying it
	//       once the issue is resolved:
	//       https://discuss.dgraph.io/t/reusing-byte-slice-in-transaction-causes-bugs-with-subscriptions/17204
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	keyBytes := unsafe.Slice(unsafe.StringData(key), len(key))
	err := b.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(keyBytes)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return txn.Set(keyBytes, valueCopy)
			}
			return err
		}

		return kvstore.ErrKeyAlreadyExists
	})

	if err != nil {
		return errors.Wrap(err, kvstore.ErrSetIfFailed.Error())
	}

	return nil
}

func (b *Badger) SetIfNotExistExpiry(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// TODO: Use value directly instead of copying it
	//       once the issue is resolved:
	//       https://discuss.dgraph.io/t/reusing-byte-slice-in-transaction-causes-bugs-with-subscriptions/17204
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	keyBytes := unsafe.Slice(unsafe.StringData(key), len(key))
	err := b.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(keyBytes)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				e := badger.NewEntry(keyBytes, valueCopy).WithTTL(ttl)
				return txn.SetEntry(e)
			}
			return err
		}

		return kvstore.ErrKeyAlreadyExists
	})

	if err != nil {
		return errors.Wrap(err, kvstore.ErrSetIfFailed.Error())
	}

	return nil
}

func (b *Badger) Delete(ctx context.Context, key string) error {
	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(unsafe.Slice(unsafe.StringData(key), len(key)))
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		return errors.Wrap(err, kvstore.ErrDeleteAllFailed.Error())
	}

	return nil
}

func (b *Badger) DeleteIf(ctx context.Context, key string, condition kvstore.Condition) error {
	keyBytes := unsafe.Slice(unsafe.StringData(key), len(key))
	err := b.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(keyBytes)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				if !condition(0) {
					return kvstore.ErrConditionFailed
				}
				return txn.Delete(keyBytes)
			}
			return errors.Wrap(err, kvstore.ErrDeleteIfFailed.Error())
		}

		if !condition(item.Version()) {
			return kvstore.ErrConditionFailed
		}

		return txn.Delete(keyBytes)
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		return errors.Wrap(err, kvstore.ErrDeleteIfFailed.Error())
	}

	return nil
}

func (b *Badger) DeleteAll(ctx context.Context, prefix string) error {
	prefixBytes := unsafe.Slice(unsafe.StringData(prefix), len(prefix))
	err := b.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			item := it.Item()
			if err := txn.Delete(item.KeyCopy(nil)); err != nil {
				if errors.Is(err, badger.ErrTxnTooBig) {
					err = txn.Commit()
					if err != nil {
						return err
					}
					txn = b.db.NewTransaction(true)
					err = txn.Delete(item.Key())
					if err != nil {
						return err
					}
					continue
				}
				return err
			}
		}
		return nil
	})

	if err != nil {
		return errors.Wrap(err, kvstore.ErrDeleteAllFailed.Error())
	}

	return nil
}

func (b *Badger) Move(ctx context.Context, oldKey string, newKey string) error {
	oldKeyBytes := unsafe.Slice(unsafe.StringData(oldKey), len(oldKey))
	newKeyBytes := unsafe.Slice(unsafe.StringData(newKey), len(newKey))
	err := b.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(oldKeyBytes)
		if err != nil {
			return err
		}
		err = txn.Delete(oldKeyBytes)
		if err != nil {
			return err
		}
		if item.ValueSize() != 0 {
			return item.Value(func(val []byte) error {
				return txn.Set(newKeyBytes, val)
			})
		}
		return txn.Set(newKeyBytes, nil)
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Wrap(kvstore.ErrKeyNotFound, kvstore.ErrMoveFailed.Error())
		}
		return errors.Wrap(err, kvstore.ErrMoveFailed.Error())
	}

	return nil
}

func (b *Badger) MoveReplace(ctx context.Context, oldKey string, newKey string, value []byte) error {
	// TODO: Use value directly instead of copying it
	//       once the issue is resolved:
	//       https://discuss.dgraph.io/t/reusing-byte-slice-in-transaction-causes-bugs-with-subscriptions/17204
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	oldKeyBytes := unsafe.Slice(unsafe.StringData(oldKey), len(oldKey))
	newKeyBytes := unsafe.Slice(unsafe.StringData(newKey), len(newKey))
	err := b.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(oldKeyBytes)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return txn.Set(newKeyBytes, valueCopy)
			}
			return err
		}

		return txn.Set(newKeyBytes, valueCopy)
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Wrap(kvstore.ErrKeyNotFound, kvstore.ErrMoveFailed.Error())
		}
		return errors.Wrap(err, kvstore.ErrMoveReplaceFailed.Error())
	}

	return nil
}

func (b *Badger) MoveReplaceIf(ctx context.Context, oldKey string, newKey string, value []byte, condition kvstore.Condition) error {
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	oldKeyBytes := unsafe.Slice(unsafe.StringData(oldKey), len(oldKey))
	newKeyBytes := unsafe.Slice(unsafe.StringData(newKey), len(newKey))
	err := b.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(oldKeyBytes)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				if !condition(0) {
					return kvstore.ErrConditionFailed
				}
				return txn.Set(newKeyBytes, valueCopy)
			}
			return err
		}

		if !condition(item.Version()) {
			return kvstore.ErrConditionFailed
		}

		err = txn.Delete(oldKeyBytes)
		if err != nil {
			return err
		}

		return txn.Set(newKeyBytes, valueCopy)
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return errors.Wrap(kvstore.ErrKeyNotFound, kvstore.ErrMoveReplaceFailed.Error())
		}
		return errors.Wrap(err, kvstore.ErrMoveReplaceFailed.Error())
	}

	return nil
}

func (b *Badger) Subscribe(ctx context.Context, prefix string, handler kvstore.SubscriptionHandler) error {
	match := pb.Match{Prefix: unsafe.Slice(unsafe.StringData(prefix), len(prefix))}
	return b.db.Subscribe(ctx, func(kvs *badger.KVList) (err error) {
		for _, kv := range kvs.Kv {
			deleted := kv.Value == nil
			err = handler(unsafe.String(unsafe.SliceData(kv.Key), len(kv.Key)), kv.Value, kv.Version, deleted)
			if err != nil {
				return errors.Wrap(err, kvstore.ErrSubscriptionFailed.Error())
			}
		}
		return
	}, []pb.Match{match})
}

func (b *Badger) Close() error {
	return b.db.Close()
}
