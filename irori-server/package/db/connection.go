package db

import (
	"database/sql"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

var DB *sql.DB

type DBConnection struct{}

func NewDBConnection() *DBConnection {
	return &DBConnection{}
}

func (*DBConnection) OpenDB() (*sql.DB, error) {
	dns := GetDBConfig()

	db, err := sql.Open("mysql", dns)
	if err != nil {
		log.Fatalf("DB接続エラー: %v", err)
		return nil, err
	}

	return db, nil
}

func (*DBConnection) OpenDBV2() (*sql.DB, error) {
	dns := GetDBConfigV2()

	db, err := sql.Open("mysql", dns)
	if err != nil {
		log.Fatalf("DB接続エラー: %v", err)
		return nil, err
	}

	return db, nil
}
