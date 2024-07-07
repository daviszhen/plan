package main

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
