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

package etcd

import (
	"context"
	"fmt"
	kvstore "github.com/loopholelabs/kvstore/pkg/kvstore"
	"github.com/loopholelabs/tls/pkg/config"
	"github.com/loopholelabs/tls/pkg/loader"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"go.etcd.io/etcd/client/pkg/v3/srv"
	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"net"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var _ kvstore.KVStore = (*ETCD)(nil)

var (
	ErrDisabled            = errors.New("etcd is disabled")
	ErrSetDelete           = errors.New("set-delete failed")
	ErrSetIfNotExist       = errors.New("set-if-not-exists failed")
	ErrInvalidBatchRequest = errors.New("invalid batch request")
	ErrBatchRequestFailed  = errors.New("batch request failed")
)

const (
	DefaultTTL = 10
)

type Options struct {
	LogName     string
	Disabled    bool
	SrvDomain   string
	ServiceName string
}

// ETCD is a wrapper for the etcd client
type ETCD struct {
	logger  *zerolog.Logger
	options *Options

	tlsConfig *config.Client
	client    *clientv3.Client
	lease     *clientv3.LeaseGrantResponse
	keepalive <-chan *clientv3.LeaseKeepAliveResponse

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(options *Options, loader loader.Loader, zapLogger *zap.Logger, logger *zerolog.Logger) (*ETCD, error) {
	l := logger.With().Str(options.LogName, "ETCD").Logger()
	if options.Disabled {
		l.Warn().Msg("disabled")
		return nil, ErrDisabled
	}

	tlsConfig, err := config.NewClient(loader, time.Hour)
	if err != nil {
		return nil, err
	}

	l.Debug().Msgf("connecting to etcd with srv-domain '%s' and service name '%s'", options.SrvDomain, options.ServiceName)
	srvs, err := srv.GetClient("etcd-client", options.SrvDomain, options.ServiceName)
	if err != nil {
		return nil, err
	}

	var endpoints []string
	for _, ep := range srvs.Endpoints {
		if strings.HasPrefix(ep, "http://") {
			l.Warn().Msgf("etcd endpoint '%s' is not using TLS, ignoring", ep)
			continue
		}
		endpoints = append(endpoints, ep)
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:            endpoints,
		AutoSyncInterval:     time.Second * DefaultTTL,
		DialTimeout:          time.Second * DefaultTTL,
		DialKeepAliveTime:    time.Second * DefaultTTL,
		DialKeepAliveTimeout: time.Second * DefaultTTL,
		TLS:                  tlsConfig.Config(),
		RejectOldCluster:     true,
		Logger:               zapLogger.With(zap.String(options.LogName, "ETCD")),
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*DefaultTTL)
	lease, err := client.Grant(ctx, DefaultTTL)
	cancel()
	if err != nil {
		return nil, err
	}

	ctx, cancel = context.WithCancel(context.Background())
	keepalive, err := client.KeepAlive(ctx, lease.ID)
	if err != nil {
		cancel()
		return nil, err
	}

	e := &ETCD{
		logger:    &l,
		options:   options,
		client:    client,
		tlsConfig: tlsConfig,
		lease:     lease,
		keepalive: keepalive,
		ctx:       ctx,
		cancel:    cancel,
	}

	e.wg.Add(1)
	go e.consumeKeepAlive()

	l.Debug().Msg("etcd client started successfully")

	return e, nil
}

func NewEmbeddded(options *Options, embedded *embed.Etcd, zapLogger *zap.Logger, logger *zerolog.Logger) (*ETCD, error) {
	l := logger.With().Str(options.LogName, "ETCD").Logger()
	if options.Disabled {
		l.Warn().Msg("disabled")
		return nil, ErrDisabled
	}
	if len(embedded.Clients) == 0 {
		return nil, errors.New("no etncd clients configured")
	}

	var clientURLs []string
	for _, listener := range embedded.Clients {
		host, port, err := net.SplitHostPort(listener.Addr().String())
		if err != nil {
			return nil, err
		}
		clientURLs = append(clientURLs, fmt.Sprintf("http://%s:%s", host, port))
	}
	if len(clientURLs) == 0 {
		return nil, errors.New("no etncd clients configured")
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:            clientURLs,
		AutoSyncInterval:     time.Second * DefaultTTL,
		DialTimeout:          time.Second * DefaultTTL,
		DialKeepAliveTime:    time.Second * DefaultTTL,
		DialKeepAliveTimeout: time.Second * DefaultTTL,
		Logger:               zapLogger.With(zap.String(options.LogName, "ETCD")),
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*DefaultTTL)
	lease, err := client.Grant(ctx, DefaultTTL)
	cancel()
	if err != nil {
		return nil, err
	}

	ctx, cancel = context.WithCancel(context.Background())
	keepalive, err := client.KeepAlive(ctx, lease.ID)
	if err != nil {
		cancel()
		return nil, err
	}

	e := &ETCD{
		logger:    &l,
		options:   options,
		client:    client,
		tlsConfig: nil,
		lease:     lease,
		keepalive: keepalive,
		ctx:       ctx,
		cancel:    cancel,
	}

	e.wg.Add(1)
	go e.consumeKeepAlive()

	l.Debug().Msg("etcd client started successfully")

	return e, nil
}

func (e *ETCD) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	res, err := e.client.Get(ctx, key)
	if err != nil {
		return nil, 0, errors.Wrap(err, kvstore.ErrGetFailed.Error())
	}
	if res.Count == 0 {
		return nil, 0, errors.Wrap(kvstore.ErrKeyNotFound, kvstore.ErrGetFailed.Error())
	}
	return res.Kvs[0].Value, uint64(res.Kvs[0].Version), nil
}

func (e *ETCD) GetFirst(ctx context.Context, prefix string) (*kvstore.Entry, error) {
	resp, err := e.client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithLimit(1))
	if err != nil {
		return nil, err
	}
	if resp.Count == 0 {
		return nil, kvstore.ErrKeyNotFound
	}

	entry := resp.Kvs[0]
	return &kvstore.Entry{
		Key:     unsafe.String(unsafe.SliceData(entry.Key), len(entry.Key)),
		Value:   entry.Value,
		Version: uint64(resp.Kvs[0].Version),
	}, nil
}

func (e *ETCD) GetAll(ctx context.Context, prefix string) (kvstore.Entries, error) {
	resp, err := e.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	entries := make(kvstore.Entries, resp.Count)
	for i, kv := range resp.Kvs {
		entries[i] = kvstore.Entry{
			Key:     unsafe.String(unsafe.SliceData(kv.Key), len(kv.Key)),
			Value:   kv.Value,
			Version: uint64(kv.Version),
		}
	}
	return entries, nil
}

func (e *ETCD) GetAllKeys(ctx context.Context, prefix string) ([]string, error) {
	resp, err := e.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, resp.Count)
	for _, kv := range resp.Kvs {
		keys = append(keys, unsafe.String(unsafe.SliceData(kv.Key), len(kv.Key)))
	}
	return keys, nil
}

func (e *ETCD) GetLimit(ctx context.Context, key string, limit int64) (kvstore.Entries, error) {
	resp, err := e.client.Get(ctx, key, clientv3.WithLimit(limit), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	entries := make(kvstore.Entries, len(resp.Kvs))
	for i, kv := range resp.Kvs {
		entries[i] = kvstore.Entry{
			Key:     unsafe.String(unsafe.SliceData(kv.Key), len(kv.Key)),
			Value:   kv.Value,
			Version: uint64(kv.Version),
		}
	}
	return entries, nil
}

func (e *ETCD) GetBatch(ctx context.Context, keys ...string) (kvstore.Entries, error) {
	if len(keys) == 0 {
		return nil, ErrInvalidBatchRequest
	}
	txn := e.client.Txn(ctx)
	ops := make([]clientv3.Op, len(keys))
	for i, key := range keys {
		ops[i] = clientv3.OpGet(key, clientv3.WithLimit(1))
	}
	txn = txn.Then(ops...)

	resp, err := txn.Commit()
	if err != nil {
		return nil, err
	}

	if !resp.Succeeded {
		return nil, ErrBatchRequestFailed
	}
	entries := make(kvstore.Entries, len(keys))
	for i, kv := range resp.Responses {
		if len(kv.GetResponseRange().Kvs) > 0 {
			entries[i] = kvstore.Entry{
				Key:     unsafe.String(unsafe.SliceData(kv.GetResponseRange().Kvs[0].Key), len(kv.GetResponseRange().Kvs[0].Key)),
				Value:   kv.GetResponseRange().Kvs[0].Value,
				Version: uint64(kv.GetResponseRange().Kvs[0].Version),
			}
		} else {
			entries[i] = kvstore.Entry{
				Key:     keys[i],
				Value:   nil,
				Version: 0,
			}
		}
	}
	return entries, nil
}

func (e *ETCD) Exists(ctx context.Context, key string) (bool, error) {
	resp, err := e.client.Get(ctx, key)
	if err != nil {
		return false, err
	}
	return resp.Count > 0, nil
}

func (e *ETCD) Set(ctx context.Context, key string, value []byte) error {
	_, err := e.client.Put(ctx, key, unsafe.String(unsafe.SliceData(value), len(value)))

	if err != nil {
		return errors.Wrap(err, kvstore.ErrSetFailed.Error())
	}
	return nil
}

func (e *ETCD) SetEmpty(ctx context.Context, key string) error {
	_, err := e.client.Put(ctx, key, "")

	if err != nil {
		return errors.Wrap(err, kvstore.ErrSetEmptyFailed.Error())
	}
	return nil
}

func (e *ETCD) SetIf(ctx context.Context, key string, value []byte, condition kvstore.Condition) error {
	_, version, err := e.Get(ctx, key)
	if err != nil && !errors.Is(err, kvstore.ErrKeyNotFound) {
		return errors.Wrap(err, kvstore.ErrSetIfFailed.Error())
	}
	if !condition(version) {
		return errors.Wrap(kvstore.ErrConditionFailed, kvstore.ErrSetIfFailed.Error())
	}
	_, err = e.client.Put(ctx, key, unsafe.String(unsafe.SliceData(value), len(value)))
	if err != nil {
		return errors.Wrap(err, kvstore.ErrSetIfFailed.Error())
	}
	return nil
}

func (e *ETCD) SetDelete(ctx context.Context, key string, value []byte, delete string) error {
	resp, err := e.client.Txn(ctx).Then(clientv3.OpPut(key, unsafe.String(unsafe.SliceData(value), len(value))), clientv3.OpDelete(delete)).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return ErrSetDelete
	}
	return nil
}

func (e *ETCD) SetKeepAlive(ctx context.Context, key string, value []byte) error {
	_, err := e.client.Put(ctx, key, unsafe.String(unsafe.SliceData(value), len(value)), clientv3.WithLease(e.lease.ID))
	return err
}

func (e *ETCD) SetExpiry(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	lease, err := e.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return err
	}
	_, err = e.client.Put(ctx, key, unsafe.String(unsafe.SliceData(value), len(value)), clientv3.WithLease(lease.ID))
	if err != nil {
		_, _ = e.client.Revoke(ctx, lease.ID)
	}
	return err
}

func (e *ETCD) SetIfNotExist(ctx context.Context, key string, value []byte) error {
	resp, err := e.client.Txn(ctx).If(clientv3.Compare(clientv3.Version(key), "=", 0)).Then(clientv3.OpPut(key, unsafe.String(unsafe.SliceData(value), len(value)))).Commit()
	if err != nil {
		return errors.Wrap(err, kvstore.ErrSetIfFailed.Error())
	}
	if !resp.Succeeded {
		return errors.Wrap(kvstore.ErrKeyAlreadyExists, kvstore.ErrSetIfFailed.Error())
	}
	return nil
}

func (e *ETCD) SetIfNotExistExpiry(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	lease, err := e.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return err
	}
	resp, err := e.client.Txn(ctx).If(clientv3.Compare(clientv3.Version(key), "=", 0)).Then(clientv3.OpPut(key, unsafe.String(unsafe.SliceData(value), len(value)), clientv3.WithLease(lease.ID))).Commit()
	if err != nil {
		_, _ = e.client.Revoke(ctx, lease.ID)
		return err
	}
	if !resp.Succeeded {
		return ErrSetIfNotExist
	}
	return err
}

func (e *ETCD) Delete(ctx context.Context, key string) error {
	_, err := e.client.Delete(ctx, key)
	if err != nil {
		return errors.Wrap(err, kvstore.ErrDeleteFailed.Error())
	}
	return nil
}

func (e *ETCD) DeleteIf(ctx context.Context, key string, condition kvstore.Condition) error {
	_, version, err := e.Get(ctx, key)
	if err != nil && !errors.Is(err, kvstore.ErrKeyNotFound) {
		return errors.Wrap(err, kvstore.ErrDeleteIfFailed.Error())
	}
	if !condition(version) {
		return errors.Wrap(kvstore.ErrConditionFailed, kvstore.ErrDeleteIfFailed.Error())
	}
	_, err = e.client.Delete(ctx, key)
	if err != nil {
		return errors.Wrap(err, kvstore.ErrDeleteIfFailed.Error())
	}
	return nil
}

func (e *ETCD) DeleteAll(ctx context.Context, prefix string) error {
	_, err := e.client.Delete(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return errors.Wrap(err, kvstore.ErrDeleteAllFailed.Error())
	}
	return nil
}

func (e *ETCD) Move(ctx context.Context, oldKey string, newKey string) error {
	val, _, err := e.Get(ctx, oldKey)
	if err != nil {
		return errors.Wrap(err, kvstore.ErrMoveFailed.Error())
	}
	resp, err := e.client.Txn(ctx).
		Then(
			clientv3.OpPut(newKey, unsafe.String(unsafe.SliceData(val), len(val))),
			clientv3.OpDelete(oldKey),
		).
		Commit()

	if err != nil {
		return errors.Wrap(err, kvstore.ErrMoveFailed.Error())
	}
	if !resp.Succeeded {
		return errors.Wrap(kvstore.ErrKeyNotFound, kvstore.ErrMoveFailed.Error())
	}
	return nil
}

func (e *ETCD) MoveReplace(ctx context.Context, oldKey string, newKey string, value []byte) error {
	resp, err := e.client.Txn(ctx).Then(clientv3.OpPut(newKey, unsafe.String(unsafe.SliceData(value), len(value))), clientv3.OpDelete(oldKey)).Commit()
	if err != nil {
		return errors.Wrap(err, kvstore.ErrMoveReplaceFailed.Error())
	}
	if !resp.Succeeded {
		return errors.Wrap(kvstore.ErrKeyNotFound, kvstore.ErrMoveReplaceFailed.Error())
	}
	return nil
}

func (e *ETCD) MoveReplaceIf(ctx context.Context, oldKey string, newKey string, value []byte, condition kvstore.Condition) error {
	_, version, err := e.Get(ctx, oldKey)
	if err != nil && !errors.Is(err, kvstore.ErrKeyNotFound) {
		return errors.Wrap(err, kvstore.ErrMoveReplaceFailed.Error())
	}
	if !condition(version) {
		return errors.Wrap(kvstore.ErrConditionFailed, kvstore.ErrMoveReplaceFailed.Error())
	}
	_, err = e.client.Txn(ctx).Then(clientv3.OpDelete(oldKey), clientv3.OpPut(newKey, unsafe.String(unsafe.SliceData(value), len(value)))).Commit()
	if err != nil {
		return errors.Wrap(err, kvstore.ErrMoveReplaceFailed.Error())
	}
	return nil
}

func (e *ETCD) Subscribe(ctx context.Context, prefix string, handler kvstore.SubscriptionHandler) error {
	ch := e.client.Watch(ctx, prefix, clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return nil
		case r, ok := <-ch:
			if !ok {
				e.logger.Warn().Msg("subscription channel closed")
				return nil
			} else if r.Err() != nil {
				// ETCD will regularly compact the revision history to save space, this throws an error in the listener
				// and we need to restart the watch
				if strings.Contains(r.Err().Error(), "required revision has been compacted") {
					e.logger.Warn().Msg("subscription revision compacted, restarting watch")
					ch = e.client.Watch(ctx, prefix, clientv3.WithPrefix())
					continue
				}
				return errors.Wrap(r.Err(), kvstore.ErrSubscriptionFailed.Error())
			}
			var err error
			for _, ev := range r.Events {
				deleted := ev.Type == clientv3.EventTypeDelete
				err = handler(unsafe.String(unsafe.SliceData(ev.Kv.Key), len(ev.Kv.Key)), ev.Kv.Value, uint64(ev.Kv.Version), deleted)
				if err != nil {
					return errors.Wrap(err, kvstore.ErrSubscriptionFailed.Error())
				}
			}

		}
	}
}

func (e *ETCD) consumeKeepAlive() {
	defer e.wg.Done()
	for {
		select {
		case r, ok := <-e.keepalive:
			if !ok {
				e.logger.Warn().Msg("keepalive channel closed")
				return
			}
			e.logger.Trace().Msgf("keepalive received for ID %d", r.ID)
		case <-e.ctx.Done():
			e.logger.Debug().Msg("closing keepalive consumer")
			return
		}
	}
}

func (e *ETCD) Close() error {
	e.logger.Debug().Msg("closing etcd client")
	e.cancel()
	if e.tlsConfig != nil {
		e.tlsConfig.Stop()
	}
	defer e.wg.Wait()
	err := e.client.Close()
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
