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

import (
	"math"

	"github.com/daviszhen/plan/pkg/util"
)

const (
	INVALID_INDEX uint32 = math.MaxUint32
)

var gConf = &util.Config{}

type Serialize interface {
	WriteData(buffer []byte, len int) error
	Close() error
}

type Deserialize interface {
	ReadData(buffer []byte, len int) error
	Close() error
}
