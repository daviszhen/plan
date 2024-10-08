// Copyright 2023-2024 daviszhen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import "github.com/BurntSushi/toml"

type Config struct {
	Format            string `tag:"format"`
	DataPath          string `tag:"dataPath"`
	ShowRaw           bool   `tag:"showRaw"`
	EnableMaxScanRows bool   `tag:"enableMaxScanRows"`
	MaxScanRows       int    `tag:"maxScanRows"`
	SkipOutput        bool   `tag:"skipOutput"`
}

var gConf = &Config{}

func init() {
	_, err := toml.DecodeFile("./config.toml", gConf)
	if err != nil {
		panic(err)
	}
}

type Serialize interface {
	WriteData(buffer []byte, len int) error
	Close() error
}

type Deserialize interface {
	ReadData(buffer []byte, len int) error
	Close() error
}
