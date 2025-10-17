package etl

import (
	"database/sql"
	"irori-server/package/db"
	"log"
	"time"
)

const (
	MAX_OPEN_CONNS    = 8
	MAX_IDEL_CONNS    = 4
	CONN_MAX_LIFETIME = (3 * time.Minute)
)

func GetDBConnections() (sourceDB, targetDB *sql.DB, err error) {
	connection := db.NewDBConnection()
	sourceDB, err = connection.OpenDB()
	if err != nil {
		return nil, nil, err
	}
	sourceDB.SetMaxOpenConns(MAX_OPEN_CONNS)       // 同時接続上限
	sourceDB.SetMaxIdleConns(MAX_IDEL_CONNS)       // アイドル保持
	sourceDB.SetConnMaxLifetime(CONN_MAX_LIFETIME) // 再利用期限（Proxy切断対策）

	targetDB, err = connection.OpenDBV2()
	if err != nil {
		log.Fatal(err)
		return nil, nil, err
	}
	targetDB.SetMaxOpenConns(MAX_OPEN_CONNS)       // 同時接続上限
	targetDB.SetMaxIdleConns(MAX_IDEL_CONNS)       // アイドル保持
	targetDB.SetConnMaxLifetime(CONN_MAX_LIFETIME) // 再利用期限（Proxy切断対策）

	return sourceDB, targetDB, nil
}
