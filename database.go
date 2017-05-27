package main

import (
	"github.com/kthomas/go-db-config"
)

func MigrateSchema() {
	db := dbconf.DatabaseConnection()
	db.AutoMigrate(&Tick{})

	db.Exec("ALTER TABLE ticks SET (autovacuum_analyze_scale_factor = 0.0);")
	db.Exec("ALTER TABLE ticks SET (autovacuum_analyze_threshold = 100000);")
	db.Exec("ALTER TABLE ticks SET (autovacuum_vacuum_scale_factor = 0.0);")
	db.Exec("ALTER TABLE ticks SET (autovacuum_vacuum_threshold = 100000);")
}
