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
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/ipfs/boxo/ipns"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	dhtopts "github.com/libp2p/go-libp2p-kad-dht/opts"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	routedhost "github.com/libp2p/go-libp2p/p2p/host/routed"
	"github.com/multiformats/go-multiaddr"
)

type LoopValidator struct {
}

// Select the first value for now...
func (lv *LoopValidator) Select(key string, values [][]byte) (int, error) {
	return 0, nil
}

// Allow anything here atm...
func (lv *LoopValidator) Validate(key string, value []byte) error {
	return nil
}

type Libp2pConfig struct {
	Pnet            string   // ID for private net
	Privkey         string   // This peers private ID
	ListenPort      int      // Port to listen on
	Bootstrap       []string // List of multiaddrs to bootstrap off
	ListenAddresses []string // List of addresses to advertise we're listening on
}

type Libp2p struct {
	Myhost   host.Host
	Mypubsub *pubsub.PubSub
	Mydht    *dht.IpfsDHT
}

func New(ctx context.Context, conf *Libp2pConfig) (*Libp2p, error) {
	l := &Libp2p{}

	var priv crypto.PrivKey

	if conf.Privkey != "" {
		privbytes, err := hex.DecodeString(conf.Privkey)
		if err != nil {
			return nil, err
		}
		priv, err = crypto.UnmarshalPrivateKey(privbytes)
		if err != nil {
			return nil, err
		}
		fmt.Printf("Using supplied p2p keypair %s\n", hex.EncodeToString(privbytes))
	} else {
		// Create a peerID to use...
		var err error
		priv, _, err = crypto.GenerateKeyPair(crypto.Secp256k1, 2048)
		if err != nil {
			return nil, err
		}

		privbytes, err := crypto.MarshalPrivateKey(priv)
		if err != nil {
			return nil, err
		}
		fmt.Printf("Generated new p2p keypair %s\n", hex.EncodeToString(privbytes))
	}
	var pnetkey []byte
	pnetkey, err := hex.DecodeString(conf.Pnet)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Connecting to p2p network using pnet key %s\n", conf.Pnet)

	loopvalid := &LoopValidator{}

	makerFunc := func(h host.Host) (routing.PeerRouting, error) {
		options := []dhtopts.Option{
			dht.NamespacedValidator("pk", record.PublicKeyValidator{}),
			dht.NamespacedValidator("ipns", ipns.Validator{KeyBook: h.Peerstore()}),
		}
		return dht.New(ctx, h, options...)
	}

	afactory := func(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
		if len(conf.ListenAddresses) > 0 {
			addrs = make([]multiaddr.Multiaddr, 0)
			for _, v := range conf.ListenAddresses {
				// Parse it into a multiaddr
				ma, err := multiaddr.NewMultiaddr(v)
				if err != nil {
					fmt.Printf("Error adding advertise addr %v", err)
				} else {
					addrs = append(addrs, ma)
				}
			}
		}
		return addrs
	}

	// Create a new host...
	basichost, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", conf.ListenPort), // regular tcp connections
			fmt.Sprintf("/ip6/::/tcp/%d", conf.ListenPort),      // regular tcp connections
		),
		//libp2p.DefaultTransports,
		libp2p.UserAgent("loopholelabs"),
		libp2p.PrivateNetwork(pnetkey),
		libp2p.Routing(makerFunc),
		//		libp2p.EnableAutoRelay(),
		libp2p.NATPortMap(),
		libp2p.AddrsFactory(afactory),
	)
	if err != nil {
		return nil, err
	}

	ds := NewTestDataStore()

	l.Mydht, err = dht.New(ctx, basichost,
		dht.Mode(dht.ModeServer),
		dht.NamespacedValidator("loophole", loopvalid),
		dht.Datastore(ds),
		dht.ProtocolPrefix(protocol.ID("/loophole")))
	if err != nil {
		return nil, err
	}

	err = l.Mydht.Bootstrap(ctx)
	if err != nil {
		return nil, err
	}

	// Wrap the host so it can route to addresses it doesn't know
	l.Myhost = routedhost.Wrap(basichost, l.Mydht)

	// Create a pubsub
	l.Mypubsub, err = pubsub.NewFloodSub(ctx, l.Myhost)

	if err != nil {
		return nil, err
	}

	for _, bs := range conf.Bootstrap {
		// Connect to the server...
		ma, err := multiaddr.NewMultiaddr(bs)
		if err != nil {
			return nil, err
		}

		peerinfo, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			return nil, err
		}

		l.Myhost.Peerstore().AddAddrs(peerinfo.ID, peerinfo.Addrs, peerstore.PermanentAddrTTL)

		// Do the connect...
		err = l.Myhost.Connect(ctx, *peerinfo)
		if err != nil {
			return nil, err
		}
	}

	return l, nil
}

func GetAdvertiseAddress(port int) (string, error) {
	client := http.Client{}

	req, err := http.NewRequest("GET", "http://ifconfig.io", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "curl/7.68.0")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", errors.New(fmt.Sprintf("IP lookup is giving us error %s", resp.Status))
	}

	bodybytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	resp.Body.Close()

	d := strings.TrimSpace(string(bodybytes))

	// Trim

	ip := net.ParseIP(d)
	if ip == nil {
		return "", errors.New(fmt.Sprintf("Could not parse IP \"%s\"", d))
	}

	// Check if it's ipv6
	if ip.To4() == nil {
		return fmt.Sprintf("/ip6/%s/tcp/%d", d, port), nil
	}
	return fmt.Sprintf("/ip4/%s/tcp/%d", d, port), nil
}
