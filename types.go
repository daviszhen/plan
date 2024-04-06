package main

import "github.com/BurntSushi/toml"

type Config struct {
	DataPath string `tag:"dataPath"`
}

var gConf = &Config{}

func init() {
	_, err := toml.DecodeFile("./config.toml", gConf)
	if err != nil {
		panic(err)
	}
}
