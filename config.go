package main

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port           int64
	DBRootUser     string
	DBRootPassword string
	DBUser         string
	DBPassword     string
	DBIP           string
	DBPort         int64
	DBName         string
}

func EnvString(name string) string {
	s := os.Getenv(name)
	if s == "" {
		panic(fmt.Sprintf("env variable '%s' is not set", name))
	}
	return s
}

func EnvInt(name string) int64 {
	s := os.Getenv(name)
	if s == "" {
		panic(fmt.Sprintf("env variable '%s' is not set", name))
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	return i
}

func EnvBool(name string) bool {
	s := os.Getenv(name)
	if s == "" {
		panic(fmt.Sprintf("env variable '%s' is not set", name))
	} else if s == "True" {
		return true
	} else if s == "False" {
		return false
	}
	panic(fmt.Sprintf("invalid value of env variable '%s'", name))
}

func GetConfig() *Config {
	l.Infoln("loading config...")

	return &Config{
		Port:       EnvInt("PORT"),
		DBUser:     EnvString("DB_USER"),
		DBPassword: EnvString("DB_PASSWORD"),
		DBIP:       EnvString("DB_IP"),
		DBPort:     EnvInt("DB_PORT"),
		DBName:     EnvString("DB_NAME"),
	}
}
