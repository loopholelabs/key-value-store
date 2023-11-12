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

package embedded

import (
	"github.com/rs/zerolog"
	"go.etcd.io/etcd/server/v3/embed"
	"net/url"
)

func New(path string, logger *zerolog.Logger) (*embed.Etcd, error) {
	cfg := embed.NewConfig()

	// 0 instructs the OS to assign a random available port
	listenUrl, err := url.Parse("http://localhost:0")
	if err != nil {
		return nil, err
	}
	cfg.ListenClientUrls = []url.URL{*listenUrl}
	cfg.ListenPeerUrls = []url.URL{*listenUrl}
	cfg.LogLevel = "error"
	cfg.Dir = path
	return embed.StartEtcd(cfg)
}
