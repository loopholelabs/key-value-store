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

package tests

import (
	"context"
	"github.com/loopholelabs/kvstore/pkg/badger"
	"github.com/loopholelabs/kvstore/pkg/etcd"
	"github.com/loopholelabs/kvstore/pkg/etcd/embedded"
	kvstore "github.com/loopholelabs/kvstore/pkg/kvstore"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	zap2 "go.uber.org/zap"
	"os"
	"testing"
	"time"
)

func RunAll(t *testing.T, fn func(db kvstore.KVStore)) {
	t.Run("BadgerDB", func(t *testing.T) {
		RunBadgerDB(t, fn)
	})
	t.Run("ETCD", func(t *testing.T) {
		RunETCD(t, fn)
	})
}

func RunBadgerDB(t *testing.T, fn func(db kvstore.KVStore)) {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.ErrorLevel)

	f, err := os.MkdirTemp("", "*")
	if err != nil {
		t.Fatal(err)
	}
	db, err := badger.New(f, &logger)
	if err != nil {
		t.Fatal(err)
	}

	fn(db)
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func RunETCD(t *testing.T, fn func(db kvstore.KVStore)) {
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.ErrorLevel)

	f, err := os.MkdirTemp("", "*")
	if err != nil {
		t.Fatal(err)
	}
	embed, err := embedded.New(f, &logger)
	if err != nil {
		t.Fatal(err)
	}

	<-embed.Server.ReadyNotify()
	opts := &etcd.Options{
		LogName:     "ETCD",
		Disabled:    false,
		SrvDomain:   "",
		ServiceName: "",
	}
	zapCfg := zap2.NewProductionConfig()
	zapCfg.Level = zap2.NewAtomicLevelAt(zap2.ErrorLevel)

	zap := zap2.Must(zapCfg.Build())
	if err != nil {
		t.Fatal(err)
	}

	db, err := etcd.NewEmbeddded(opts, embed, zap, &logger)
	if err != nil {
		t.Fatal(err)
	}
	fn(db)
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetSet(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.Set(ctx, "foo", []byte("bar"))
		assert.Nil(t, err)

		val, ver, err := db.Get(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, "bar", string(val))
		assert.Equal(t, uint64(1), ver)
	})
}
func TestGetFirst(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.Set(ctx, "foo123", []byte("bar"))
		assert.Nil(t, err)

		entry, err := db.GetFirst(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, "foo123", entry.Key)
		assert.Equal(t, "bar", string(entry.Value))
		assert.Equal(t, uint64(1), entry.Version)
	})
}

func TestGetAll(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.Set(ctx, "foo1", []byte("bar"))
		assert.Nil(t, err)
		err = db.Set(ctx, "foo2", []byte("baz"))
		assert.Nil(t, err)

		entries, err := db.GetAll(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, 2, len(entries))
		assert.Equal(t, "foo1", entries[0].Key)
		assert.Equal(t, "bar", string(entries[0].Value))
		assert.Equal(t, "foo2", entries[1].Key)
		assert.Equal(t, "baz", string(entries[1].Value))
	})
}

func TestGetAllKeys(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.Set(ctx, "foo1", []byte("bar"))
		assert.Nil(t, err)
		err = db.Set(ctx, "foo2", []byte("baz"))
		assert.Nil(t, err)

		keys, err := db.GetAllKeys(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, 2, len(keys))
		assert.Equal(t, "foo1", keys[0])
		assert.Equal(t, "foo2", keys[1])
	})
}

func TestGetLimit(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.Set(ctx, "foo1", []byte("bar"))
		assert.Nil(t, err)
		err = db.Set(ctx, "foo2", []byte("baz"))
		assert.Nil(t, err)
		err = db.Set(ctx, "foo3", []byte("baz"))
		assert.Nil(t, err)

		entries, err := db.GetLimit(ctx, "foo", 2)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(entries))
	})
}

func TestGetBatch(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.Set(ctx, "foo", []byte("1"))
		assert.Nil(t, err)
		err = db.Set(ctx, "bar", []byte("2"))
		assert.Nil(t, err)
		err = db.Set(ctx, "baz", []byte("3"))
		assert.Nil(t, err)

		entries, err := db.GetBatch(ctx, "foo", "bar", "baz")
		assert.Nil(t, err)
		assert.Equal(t, 3, len(entries))
	})
}

func TestExists(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.Set(ctx, "foo", []byte("bar"))
		assert.Nil(t, err)

		exists, err := db.Exists(ctx, "foo")
		assert.Nil(t, err)
		assert.True(t, exists)
	})
}

func TestSetEmpty(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.Set(ctx, "foo", nil)
		assert.Nil(t, err)

		val, ver, err := db.Get(ctx, "foo")
		assert.Nil(t, err)
		assert.Nil(t, val)
		assert.Equal(t, uint64(1), ver)
	})
}

func TestSetIf(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.SetIf(ctx, "foo", []byte("bar"), func(v uint64) bool {
			return v == 0
		})
		assert.Nil(t, err)

		val, ver, err := db.Get(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, "bar", string(val))
		assert.Equal(t, uint64(1), ver)

		err = db.SetIf(ctx, "foo", []byte("bar"), func(v uint64) bool {
			return v == 0
		})
		assert.Equal(t, "error during set-if: error during condition", err.Error())
	})
}

func TestSetDelete(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.Set(ctx, "foo", []byte("bar"))
		assert.Nil(t, err)

		err = db.SetDelete(ctx, "baz", []byte("bar"), "foo")
		assert.Nil(t, err)

		val, ver, err := db.Get(ctx, "foo")
		assert.Nil(t, val)
		assert.Equal(t, "error during get: key not found in storage", err.Error())
		assert.Equal(t, uint64(0), ver)

		val, _, err = db.Get(ctx, "baz")
		assert.Nil(t, err)
		assert.Equal(t, "bar", string(val))
	})
}

func TestSetExpiry(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.SetExpiry(ctx, "foo", []byte("bar"), time.Second*1)
		assert.Nil(t, err)

		val, _, err := db.Get(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, "bar", string(val))

		// etcd is slow to release keys after expiry, this test will not reliably pass with a lower wait time
		time.Sleep(time.Second * 3)

		val, _, err = db.Get(ctx, "foo")
		assert.Nil(t, val)
		assert.Equal(t, "error during get: key not found in storage", err.Error())
	})
}

func TestSetIfNotExist(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.SetIfNotExist(ctx, "foo", []byte("bar"))
		assert.Nil(t, err)

		val, ver, err := db.Get(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, "bar", string(val))
		assert.Equal(t, uint64(1), ver)

		err = db.SetIfNotExist(ctx, "foo", []byte("bar"))
		assert.Equal(t, "error during set-if: key already exists in storage", err.Error())
	})
}

func TestSetIfNotExistExpiry(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.SetIfNotExistExpiry(ctx, "foo", []byte("bar"), time.Second*1)
		assert.Nil(t, err)

		val, _, err := db.Get(ctx, "foo")
		assert.Nil(t, err)
		assert.Equal(t, "bar", string(val))

		// etcd is slow to release keys after expiry, this test will not reliably pass with a lower wait time
		time.Sleep(time.Second * 3)

		val, _, err = db.Get(ctx, "foo")
		assert.Nil(t, val)
		assert.Equal(t, "error during get: key not found in storage", err.Error())
	})
}

func TestDelete(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.Set(ctx, "foo", []byte("bar"))
		assert.Nil(t, err)

		err = db.Delete(ctx, "foo")
		assert.Nil(t, err)

		val, ver, err := db.Get(ctx, "foo")
		assert.Nil(t, val)
		assert.Equal(t, "error during get: key not found in storage", err.Error())
		assert.Equal(t, uint64(0), ver)
	})
}

func TestDeleteIf(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.Set(ctx, "foo", []byte("bar"))
		assert.Nil(t, err)

		err = db.DeleteIf(ctx, "foo", func(v uint64) bool {
			return v == 1
		})
		assert.Nil(t, err)

		val, ver, err := db.Get(ctx, "foo")
		assert.Nil(t, val)
		assert.Equal(t, "error during get: key not found in storage", err.Error())
		assert.Equal(t, uint64(0), ver)

		err = db.DeleteIf(ctx, "foo", func(v uint64) bool {
			return v == 1
		})
		assert.Equal(t, "error during delete-if: error during condition", err.Error())
	})
}

func TestDeleteAll(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.Set(ctx, "foo1", []byte("bar"))
		assert.Nil(t, err)

		err = db.DeleteAll(ctx, "foo")
		assert.Nil(t, err)

		val, ver, err := db.Get(ctx, "foo1")
		assert.Equal(t, "error during get: key not found in storage", err.Error())
		assert.Nil(t, val)
		assert.Equal(t, uint64(0), ver)
	})
}

func TestMove(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.Set(ctx, "foo", []byte("bar"))
		assert.Nil(t, err)

		err = db.Move(ctx, "foo", "bar")
		assert.Nil(t, err)

		val, ver, err := db.Get(ctx, "foo")
		assert.Equal(t, "error during get: key not found in storage", err.Error())
		assert.Nil(t, val)
		assert.Equal(t, uint64(0), ver)

		val, _, err = db.Get(ctx, "bar")
		assert.Nil(t, err)
		assert.Equal(t, "bar", string(val))
	})
}

func TestMoveReplace(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.Set(ctx, "foo", []byte("bar"))
		assert.Nil(t, err)

		err = db.MoveReplace(ctx, "foo", "bar", []byte("baz"))
		assert.Nil(t, err)

		val, ver, err := db.Get(ctx, "foo")
		assert.Equal(t, "error during get: key not found in storage", err.Error())
		assert.Nil(t, val)
		assert.Equal(t, uint64(0), ver)

		val, _, err = db.Get(ctx, "bar")
		assert.Nil(t, err)
		assert.Equal(t, "baz", string(val))
	})
}

func TestMoveReplaceIf(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		ctx := context.Background()

		err := db.Set(ctx, "foo", []byte("bar"))
		assert.Nil(t, err)

		err = db.MoveReplaceIf(ctx, "foo", "bar", []byte("baz"), func(v uint64) bool {
			return v == 1
		})
		assert.Nil(t, err)

		val, ver, err := db.Get(ctx, "foo")
		assert.Equal(t, "error during get: key not found in storage", err.Error())
		assert.Nil(t, val)
		assert.Equal(t, uint64(0), ver)

		val, _, err = db.Get(ctx, "bar")
		assert.Nil(t, err)
		assert.Equal(t, "baz", string(val))

		err = db.MoveReplaceIf(ctx, " bar", "baz", []byte("foo"), func(v uint64) bool {
			return v == 1
		})
		assert.Equal(t, "error during move-replace: error during condition", err.Error())
	})
}

func TestSubscribe(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		received := make(chan bool, 1)
		handler := func(key string, value []byte, version uint64, deleted bool) error {
			if key == "foo" && string(value) == "bar" && deleted == false {
				received <- true
			}
			return nil
		}
		go func() {
			err := db.Subscribe(context.Background(), "foo", handler)
			if err != nil {
				t.Errorf("Subscribe failed: %v", err)
			}
		}()

		err := db.Set(context.Background(), "foo", []byte("bar"))
		if err != nil {
			t.Errorf("Set failed: %v", err)
		}

		select {
		case success := <-received:
			assert.Equal(t, true, success)
			break
		case <-time.After(time.Second * 10):
			t.Errorf("Handler was not invoked")
			break
		}
	})
}

func TestSubscribeDelete(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		received := make(chan bool, 1)
		handler := func(key string, value []byte, version uint64, deleted bool) error {
			if key == "foo" && value == nil && deleted == true {
				received <- true
			}
			return nil
		}
		go func() {
			err := db.Subscribe(context.Background(), "foo", handler)
			if err != nil {
				t.Errorf("Subscribe failed: %v", err)
			}
		}()

		err := db.Set(context.Background(), "foo", []byte("bar"))
		if err != nil {
			t.Errorf("Set failed: %v", err)
		}

		err = db.Delete(context.Background(), "foo")
		if err != nil {
			t.Errorf("Set failed: %v", err)
		}

		select {
		case success := <-received:
			assert.Equal(t, true, success)
			break
		case <-time.After(time.Second * 20):
			t.Errorf("Handler was not invoked")
			break
		}
	})
}

func TestSubscribeExpiringKey(t *testing.T) {
	RunAll(t, func(db kvstore.KVStore) {
		received := make(chan bool, 1)
		handler := func(key string, value []byte, version uint64, deleted bool) error {
			if key == "foo" && value == nil && deleted == true {
				received <- true
			}
			return nil
		}
		go func() {
			err := db.Subscribe(context.Background(), "foo", handler)
			if err != nil {
				t.Errorf("Subscribe failed: %v", err)
			}
		}()

		err := db.SetExpiry(context.Background(), "foo", []byte("bar"), time.Second*1)
		if err != nil {
			t.Errorf("Set failed: %v", err)
		}

		err = db.Delete(context.Background(), "foo")
		if err != nil {
			t.Errorf("Set failed: %v", err)
		}

		select {
		case success := <-received:
			assert.Equal(t, true, success)
			break
		case <-time.After(time.Second * 20):
			t.Errorf("Handler was not invoked")
			break
		}
	})
}
