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

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"go.uber.org/zap"

	"github.com/daviszhen/plan/pkg/plan"
	"github.com/daviszhen/plan/pkg/util"
)

func init() {
	cobra.OnInitialize(loadConfig)
	initTpch1gCmd()
}

var testerCfg = &util.Config{}

///root cmd

var info = "tester"
var RootCmd = &cobra.Command{
	Use:          "tester",
	Short:        info,
	Long:         info,
	SilenceUsage: true,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("use tester --help or -h")
	},
}

func initDebugOptions() {
	testerCfg.Debug.ShowRaw = viper.GetBool("debug.showRaw")
	testerCfg.Debug.EnableMaxScanRows = viper.GetBool("debug.enableMaxScanRows")
	testerCfg.Debug.MaxScanRows = viper.GetInt("debug.maxScanRows")
	testerCfg.Debug.MaxOutputRowCount = viper.GetInt("debug.maxOutputRowCount")
	testerCfg.Debug.PrintResult = viper.GetBool("debug.printResult")
	testerCfg.Debug.PrintPlan = viper.GetBool("debug.printPlan")
	testerCfg.Debug.Count = viper.GetInt("debug.count")
}

//tpch1g cmd

var tpch1gInfo = "run tpch1g query"
var tpch1gCmd = &cobra.Command{
	Use:   "tpch1g",
	Short: tpch1gInfo,
	Long:  tpch1gInfo,
	RunE: func(cmd *cobra.Command, args []string) error {
		initTpch1gCfg()
		return plan.Run(testerCfg)
	},
}

func initTpch1gCfg() {
	initDebugOptions()
	testerCfg.Tpch1g.Query.QueryId = viper.GetUint("tpch1g.query.queryId")
	testerCfg.Tpch1g.Query.Path = viper.GetString("tpch1g.query.path")
	testerCfg.Tpch1g.Data.Path = viper.GetString("tpch1g.data.path")
	testerCfg.Tpch1g.Data.Format = viper.GetString("tpch1g.data.format")
	testerCfg.Tpch1g.Result.Path = viper.GetString("tpch1g.result.path")
	testerCfg.Tpch1g.Result.NeedHeadLine = viper.GetBool("tpch1g.result.needHeadline")
}

func initTpch1gCmd() {
	RootCmd.AddCommand(tpch1gCmd)
	tpch1gCmd.Flags().UintVar(&testerCfg.Tpch1g.Query.QueryId, "query_id", 0, "query id")
	tpch1gCmd.Flags().StringVar(&testerCfg.Tpch1g.Data.Path, "data_path", "", "tpch 1g data path")
	tpch1gCmd.Flags().StringVar(&testerCfg.Tpch1g.Data.Format, "data_format", "", "tpch 1g data format. csv, parquet")
	tpch1gCmd.Flags().StringVar(&testerCfg.Tpch1g.Result.Path, "result_path", "", "query result path")
	tpch1gCmd.Flags().BoolVar(&testerCfg.Tpch1g.Result.NeedHeadLine, "need_headline", true, "output headline in query result")

	viper.BindPFlag("tpch1g.query.queryId", tpch1gCmd.Flags().Lookup("query_id"))
	viper.BindPFlag("tpch1g.data.path", tpch1gCmd.Flags().Lookup("data_path"))
	viper.BindPFlag("tpch1g.data.format", tpch1gCmd.Flags().Lookup("data_format"))
	viper.BindPFlag("tpch1g.result.path", tpch1gCmd.Flags().Lookup("result_path"))
	viper.BindPFlag("tpch1g.result.needHeadline", tpch1gCmd.Flags().Lookup("need_headline"))
}

var defCfgFilePaths = []string{".", "etc/tpch/1g"}
var cfgFileName = "tester.toml"

func loadConfig() {
	has := false
	for _, dirPath := range defCfgFilePaths {
		fpath := filepath.Join(dirPath, cfgFileName)
		if util.FileIsValid(fpath) {
			viper.SetConfigFile(fpath)
			err := viper.ReadInConfig()
			if err != nil {
				util.Error("viper load config file failed",
					zap.String("fpath", fpath),
					zap.Error(err))
				continue
			}
			has = true
			break
		}
	}
	if !has {
		util.Error("tester.toml does not exist")
		os.Exit(1)
	}
}

func main() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
