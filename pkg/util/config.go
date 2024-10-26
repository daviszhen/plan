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

package util

type Tpch1gData struct {
	Path   string `tag:"path"`
	Format string `tag:"format"`
}

type Tpch1gResult struct {
	Path         string `tag:"path"`
	NeedHeadLine bool   `tag:"needHeadline"`
}

type Tpch1g struct {
	QueryId uint         `tag:"queryId"`
	Data    Tpch1gData   `tag:"data"`
	Result  Tpch1gResult `tag:"result"`
}

type DebugOptions struct {
	ShowRaw           bool `tag:"showRaw"`
	EnableMaxScanRows bool `tag:"enableMaxScanRows"`
	MaxScanRows       int  `tag:"maxScanRows"`
	MaxOutputRowCount int  `tag:"maxOutputRowCount"`
	PrintResult       bool `tag:"printResult"`
	PrintPlan         bool `tag:"printPlan"`
}

type Config struct {
	Tpch1g Tpch1g       `tag:"tpch1g"`
	Debug  DebugOptions `tag:"debug"`
}
