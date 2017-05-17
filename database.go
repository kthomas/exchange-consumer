package main

import (
	"github.com/kthomas/go-db-config"
)

func MigrateSchema() {
	db := dbconf.DatabaseConnection()
	db.AutoMigrate(&Tick{})
}
