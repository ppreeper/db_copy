package main

import (
	"os"

	dbc "github.com/ppreeper/db_copy/database"
	"gopkg.in/yaml.v2"
)

// Dbase for loading from json
// type Dbase struct {
// 	Name     string   `json:"name,omitempty"`
// 	Driver   string   `json:"driver,omitempty"`
// 	Host     string   `json:"host,omitempty"`
// 	Port     string   `json:"port,omitempty"`
// 	Database string   `json:"database,omitempty"`
// 	Schema   []string `json:"schema,omitempty"`
// 	Username string   `json:"username,omitempty"`
// 	Password string   `json:"password,omitempty"`
// 	PoolSize string   `json:"poolsize,omitempty"`
// }

// Conf array of Dbase
type Conf struct {
	Dbases []dbc.Database `json:"dbases,omitempty"`
}

func (c *Conf) getConf(configFile string) (*Conf, error) {
	yamlFile, err := os.ReadFile(configFile)
	checkErr(err)
	err = yaml.Unmarshal(yamlFile, c)
	checkErr(err)
	return c, err
}

func (c *Conf) getDB(name string) (d dbc.Database) {
	for _, v := range c.Dbases {
		if v.Name == name {
			d = v
		}
	}
	return
}
