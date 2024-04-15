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

// Package KVStore outlines how the key value store interface should be defined
package kvstore

import (
	"context"
	"errors"
	"time"
)

var (
	ErrKeyNotFound       = errors.New("key not found in storage")
	ErrSetFailed         = errors.New("error during set")
	ErrSetEmptyFailed    = errors.New("error during set-empty")
	ErrSetIfFailed       = errors.New("error during set-if")
	ErrGetFailed         = errors.New("error during get")
	ErrDeleteFailed      = errors.New("error during delete")
	ErrMoveFailed        = errors.New("error during move")
	ErrMoveReplaceFailed = errors.New("error during move-replace")
	ErrGetFirstFailed    = errors.New("error during get-first")
	ErrGetAllFailed      = errors.New("error during get-all")
	ErrGetBatchFailed    = errors.New("error during get-batch")
	ErrGetAllKeysFailed  = errors.New("error during get-all-keys")
	ErrDeleteAllFailed   = errors.New("error during delete-all")
	ErrDeleteIfFailed    = errors.New("error during delete-if")

	ErrSubscriptionFailed = errors.New("error during subscription")
	ErrConditionFailed    = errors.New("error during condition")
	ErrKeyAlreadyExists   = errors.New("key already exists in storage")
)

type KVStore interface {
	// Get retrieves a value given a key. If a key is not found, this method should return NotFoundError,
	// and any other returned errors should be wrapped in GetError.
	Get(ctx context.Context, key string) (value []byte, version uint64, err error)

	// GetFirst gets the first entry from the DB that matches the prefix. If an error is returned it
	// should be wrapped in GetFirstError.
	//
	// This function expects the keys to be ordered byte-wise lexicographically.
	// For reference: https://pkg.go.dev/bytes#Compare
	GetFirst(ctx context.Context, prefix string) (entry *Entry, err error)

	// GetAll gets all the keys and their values that are associated with a prefix. If an error is returned it
	// should be wrapped in GetAllError.
	GetAll(ctx context.Context, prefix string) (entries Entries, err error)

	// GetAllKeys gets all the keys associated with a prefix. If an error is returned it
	// should be wrapped in GetAllKeysError.
	GetAllKeys(ctx context.Context, prefix string) (keys []string, err error)

	// GetLimit gets all the keys and their values that are associated with a prefix, up to a given limit.
	GetLimit(ctx context.Context, prefix string, limit int64) (entries Entries, err error)

	// GetBatch takes a list of keys and returns a list of corresponding entries.
	// The slice index should correspond to the requested key, if there is no value for a requested key an empty entry
	// is placed at the index.
	GetBatch(ctx context.Context, keys ...string) (entries Entries, err error)

	// Exists checks if a given key exists in the database. If the key is not found or an error occurs,
	// this function returns false - otherwise it returns true.
	Exists(ctx context.Context, key string) (exists bool, err error)

	// Set sets a value given a key. If an error is returned it should be wrapped in SetError.
	Set(ctx context.Context, key string, value []byte) (err error)

	// SetEmpty sets an empty value given a key. If an error is returned it should be wrapped in SetEmptyError.
	SetEmpty(ctx context.Context, key string) (err error)

	// SetIf sets a value given a key and a condition. If the condition is not met,
	// this method should return ConditionError, and any other returned errors should be wrapped in SetError.
	SetIf(ctx context.Context, key string, value []byte, condition Condition) (err error)

	// SetDelete Sets a key to a value and deletes an entry at another key.
	//If an error is returned it should be wrapped in SetDeleteError.
	SetDelete(ctx context.Context, key string, value []byte, delete string) (err error)

	// SetExpiry sets a value given a key and a ttl, the entry will automatically be deleted at the end of the ttl.
	// ttl should be given in whole seconds, milliseconds may be truncated in certain implementations.
	// If an error is returned it should be wrapped in SetError.
	SetExpiry(ctx context.Context, key string, value []byte, ttl time.Duration) error

	// SetIfNotExist sets a value given a key if the key does not already exist.
	// If an error is returned it should be wrapped in SetError.
	SetIfNotExist(ctx context.Context, key string, value []byte) (err error)

	// SetIfNotExistExpiry sets a value given a key if the key does not already exist and sets an expiry.
	// The entry will automatically be deleted at the end of the ttl.
	// ttl should be given in whole seconds, milliseconds may be truncated in certain implementations.
	SetIfNotExistExpiry(ctx context.Context, key string, value []byte, ttl time.Duration) (err error)

	// Delete deletes a value given a key. If a key is not found, this method should not return an error,
	// and any other returned errors should be wrapped in DeleteError.
	Delete(ctx context.Context, key string) (err error)

	// DeleteIf deletes a value given a key and a condition. If the condition is not met,
	// this method should return ConditionError, and any other returned errors should be wrapped in DeleteError.
	DeleteIf(ctx context.Context, key string, condition Condition) (err error)

	// DeleteAll deletes all the keys associated with a prefix. If an error is returned it
	// should be wrapped in DeleteAllError.
	DeleteAll(ctx context.Context, prefix string) (err error)

	// Move retrieves a key and then sets that value under a new key. If an error is returned it
	// should be wrapped in MoveError.
	Move(ctx context.Context, oldKey string, newKey string) (err error)

	// MoveReplace deletes a key and then sets a new key with a new value. If an error is returned it
	// should be wrapped in MoveReplaceError.
	MoveReplace(ctx context.Context, oldKey string, newKey string, value []byte) (err error)

	// MoveReplaceIf deletes a key if the given condition returns true, and then sets a new key with a new value.
	// If the condition is not met, this method should return ConditionError, and any other returned errors
	// should be wrapped in MoveReplaceError.
	MoveReplaceIf(ctx context.Context, oldKey string, newKey string, value []byte, condition Condition) (err error)

	// Subscribe subscribes to updates for keys under a given prefix. If an error is returned it
	// should be wrapped in SubscriptionError.
	//
	// It's important to note that some implementations of subscriptions cannot distinguish between deleting a key
	//and setting an empty value. Because of this, it is recommended to not depend on subscriptions for keys that may
	// contain empty values.
	Subscribe(ctx context.Context, prefix string, handler SubscriptionHandler) error

	// Close gracefully shuts the storage engine down.
	Close() error
}

// Entry is a single entry in the DB
type Entry struct {
	// Key is the key for an Entry
	Key string

	// Value is the value for an Entry
	Value []byte

	// Version is the version of the entry
	Version uint64
}

// Entries is a list of multiple Entry structs
type Entries []Entry

// SubscriptionHandler is a function that is called when a subscription is triggered
type SubscriptionHandler func(key string, value []byte, version uint64, deleted bool) error

// Condition is a function that returns a boolean value indicating if the condition is met
type Condition func(version uint64) (condition bool)
