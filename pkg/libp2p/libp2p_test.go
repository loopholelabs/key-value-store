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

package libp2p

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
)

func TestLibp2pKVStore(t *testing.T) {
	pnet := make([]byte, 32)
	rand.Read(pnet)

	c1 := &Libp2pConfig{
		Pnet:            hex.EncodeToString(pnet),
		Privkey:         "",
		ListenPort:      22022,
		Bootstrap:       make([]string, 0),
		ListenAddresses: make([]string, 0),
	}

	c2 := &Libp2pConfig{
		Pnet:            hex.EncodeToString(pnet),
		Privkey:         "",
		ListenPort:      22033,
		Bootstrap:       make([]string, 0),
		ListenAddresses: make([]string, 0),
	}

	ctx, cancelfunc := context.WithCancel(context.Background())

	l1, err := New(ctx, c1)
	assert.NoError(t, err)

	l2, err := New(ctx, c2)
	assert.NoError(t, err)

	t.Cleanup(func() {
		cancelfunc()
	})

	host2 := peer.AddrInfo{
		ID:    l2.Myhost.ID(),
		Addrs: l2.Myhost.Addrs(),
	}

	err = l1.Myhost.Connect(ctx, host2)
	assert.NoError(t, err)

	// peer1 should be connected to peer2 now...

	// Wait until dht1 picks up the other peer...
	for {
		peers := len(l1.Mydht.RoutingTable().ListPeers())
		if peers > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Now we need to do some store / load ops...

	key1 := "/loophole/loohole-123"
	value1 := []byte("Hello world")

	err = l1.Mydht.PutValue(ctx, key1, value1)
	assert.NoError(t, err)

	key2 := "/loophole/loohole-456"
	value2 := []byte("Goodbye village")

	err = l1.Mydht.PutValue(ctx, key2, value2)
	assert.NoError(t, err)

	// Look it up on both nodes
	val1_1, err := l1.Mydht.GetValue(ctx, key1)
	assert.NoError(t, err)
	assert.Equal(t, value1, val1_1)

	val1_2, err := l1.Mydht.GetValue(ctx, key2)
	assert.NoError(t, err)
	assert.Equal(t, value2, val1_2)

	// Look it up on both nodes
	val2_1, err := l2.Mydht.GetValue(ctx, key1)
	assert.NoError(t, err)
	assert.Equal(t, value1, val2_1)

	val2_2, err := l2.Mydht.GetValue(ctx, key2)
	assert.NoError(t, err)
	assert.Equal(t, value2, val2_2)

}

func TestLibp2pPubsub(t *testing.T) {
	pnet := make([]byte, 32)
	rand.Read(pnet)

	c1 := &Libp2pConfig{
		Pnet:            hex.EncodeToString(pnet),
		Privkey:         "",
		ListenPort:      22022,
		Bootstrap:       make([]string, 0),
		ListenAddresses: make([]string, 0),
	}

	c2 := &Libp2pConfig{
		Pnet:            hex.EncodeToString(pnet),
		Privkey:         "",
		ListenPort:      22033,
		Bootstrap:       make([]string, 0),
		ListenAddresses: make([]string, 0),
	}

	ctx, cancelfunc := context.WithCancel(context.Background())

	l1, err := New(ctx, c1)
	assert.NoError(t, err)

	l2, err := New(ctx, c2)
	assert.NoError(t, err)

	t.Cleanup(func() {
		cancelfunc()
	})

	host2 := peer.AddrInfo{
		ID:    l2.Myhost.ID(),
		Addrs: l2.Myhost.Addrs(),
	}

	err = l1.Myhost.Connect(ctx, host2)
	assert.NoError(t, err)

	// peer1 should be connected to peer2 now...

	topic1, err := l1.Mypubsub.Join("/loophole/topic1")
	assert.NoError(t, err)

	topic2, err := l2.Mypubsub.Join("/loophole/topic1")
	assert.NoError(t, err)

	sub1, err := topic1.Subscribe()
	assert.NoError(t, err)

	sub2, err := topic2.Subscribe()
	assert.NoError(t, err)

	// Send something
	data := []byte("Hello world")
	err = topic1.Publish(ctx, data)
	assert.NoError(t, err)

	msg1, err := sub1.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, data, msg1.Data)
	assert.Equal(t, []byte(l1.Myhost.ID()), msg1.From)

	msg2, err := sub2.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, data, msg2.Data)
	assert.Equal(t, []byte(l1.Myhost.ID()), msg2.From)

}
