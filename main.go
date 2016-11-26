package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/kevinburke/ansible-go/mysql"
	"github.com/kevinburke/ansible-go/ssh"

	yaml "gopkg.in/yaml.v2"
)

type Config struct {
	DBUser     string `yaml:"database_user"`
	DBPassword string `yaml:"database_password"`
	DBName     string `yaml:"database_name"`
	OldHost    string `yaml:"old_host"`
	NewHost    string `yaml:"new_host"`
	User       string `yaml:"user"`
}

func main() {
	cfg := flag.String("config", "config.yml", "Path to a config file")
	flag.Parse()

	data, err := ioutil.ReadFile(*cfg)
	if err != nil {
		log.Fatal(err)
	}
	c := new(Config)
	if err := yaml.Unmarshal(data, c); err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmp, err := ioutil.TempFile("", "db-backup-")
	if err != nil {
		log.Fatal(err)
	}
	defer tmp.Close()
	os.Stdout.WriteString("downloading db to " + tmp.Name() + "\n")
	host := ssh.Host{
		Name: c.OldHost,
		User: "",
	}
	mycfg := mysql.CommandConfig{
		Host:     "localhost",
		Port:     "3306",
		User:     c.DBUser,
		Password: c.DBPassword,
	}
	dumpcfg := mysql.DumpConfig{CommandConfig: mycfg, SingleTransaction: true}

	writer := bufio.NewWriter(tmp)
	if err := mysql.DumpWriter(ctx, host, c.DBName, writer, dumpcfg); err != nil {
		log.Fatal(err)
	}

	if err := writer.Flush(); err != nil {
		log.Fatal(err)
	}

	createCfg := mysql.CreateConfig{
		Password: c.DBPassword,
		Privilege: mysql.Privilege{
			Database:   c.DBName,
			Privileges: []string{"ALL"},
		},
	}
	newHost := ssh.Host{
		Name: c.NewHost,
		User: "",
	}
	if err := mysql.CreateUser(ctx, newHost, c.DBUser, createCfg); err != nil {
		log.Fatal(err)
	}

	f, err := os.Open(tmp.Name())
	if err != nil {
		log.Fatal(err)
	}
	if err := mysql.RunCommands(ctx, newHost, c.DBName, f, mycfg); err != nil {
		log.Fatal(err)
	}
	fmt.Println("imported data to new host")
}
