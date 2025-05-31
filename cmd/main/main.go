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
	"context"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	wire "github.com/jeroenrinzema/psql-wire"
	"go.uber.org/zap"

	"github.com/daviszhen/plan/pkg/compute"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

var runCfg util.Config

func init() {
	loadConfig()
}

var defCfgFilePaths = []string{".", "etc/tpch/1g"}
var cfgFileName = "tester.toml"

func loadConfig() {
	has := false
	for _, dirPath := range defCfgFilePaths {
		fpath := filepath.Join(dirPath, cfgFileName)
		if util.FileIsValid(fpath) {
			_, err := toml.DecodeFile(fpath, &runCfg)
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
	wire.ListenAndServe("127.0.0.1:5432", handler)
}

func handler(ctx context.Context, query string) (wire.PreparedStatements, error) {
	util.Info("incoming SQL :", zap.String("query", query))
	var err error
	txn, err := storage.GTxnMgr.NewTxn("handler")
	if err != nil {
		return nil, err
	}
	storage.BeginQuery(txn)

	//init runner
	run, err := compute.InitRunner(&runCfg, txn, query)
	if err != nil {
		return nil, err
	}
	execCtx := ExecCtx{
		cfg: &runCfg,
		run: run,
	}

	//gen columns
	cols := execCtx.run.Columns()

	return wire.Prepared(
		wire.NewStatement(execCtx.handleX,
			wire.WithColumns(cols),
		),
	), nil
}

type ExecCtx struct {
	cfg *util.Config
	run *compute.Runner
}

func (exec *ExecCtx) handleX(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
	var err error
	defer exec.run.Close()
	defer func() {
		if err != nil {
			storage.GTxnMgr.Rollback(exec.run.Txn)
		} else {
			err = storage.GTxnMgr.Commit(exec.run.Txn)
		}
	}()

	//run stmt
	err = exec.run.Run(ctx, writer)
	if err != nil {
		return err
	}
	return writer.Complete("")
}
