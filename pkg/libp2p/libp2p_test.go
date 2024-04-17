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
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
)

func TestLibp2p(t *testing.T) {
	c1 := &Libp2pConfig{
		Pnet:            "08021220050fc6dcef51c6b36f64237d8f7e45f45e7f86bd47e955d4b733c31434099708",
		Privkey:         "",
		ListenPort:      22022,
		Bootstrap:       make([]string, 0),
		ListenAddresses: make([]string, 0),
	}

	c2 := &Libp2pConfig{
		Pnet:            "08021220050fc6dcef51c6b36f64237d8f7e45f45e7f86bd47e955d4b733c31434099708",
		Privkey:         "",
		ListenPort:      22033,
		Bootstrap:       make([]string, 0),
		ListenAddresses: make([]string, 0),
	}

	//	c1.ListenAddresses = append(c1.ListenAddresses, "/ip4/127.0.0.1/tcp/22022")
	//c1.Bootstrap = append(c1.Bootstrap, "/ip4/127.0.0.1/tcp/22033")

	//	c2.ListenAddresses = append(c1.ListenAddresses, "/ip4/127.0.0.1/tcp/22033")
	//c2.Bootstrap = append(c1.Bootstrap, "/ip4/127.0.0.1/tcp/22022")

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

	for _, con := range l1.Myhost.Network().Conns() {
		fmt.Printf("host1 Connection: %v\n", con)
	}

	for _, con := range l2.Myhost.Network().Conns() {
		fmt.Printf("host2 Connection: %v\n", con)
	}

	// TODO: Get rid of this somehow... need to know when dht is ready
	time.Sleep(2 * time.Second)

	// Wait until they're both connected?
	e1 := l1.Mydht.RefreshRoutingTable()
	e2 := l2.Mydht.RefreshRoutingTable()

	err = <-e1
	assert.NoError(t, err)
	err = <-e2
	assert.NoError(t, err)

	// Now we need to do some store / load ops...

	key := "/loophole/loohole-123"
	value := []byte("Hello world")

	err = l1.Mydht.PutValue(ctx, key, value)
	assert.NoError(t, err)

	// Look it up
	val, err := l1.Mydht.GetValue(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, value, val)

}
