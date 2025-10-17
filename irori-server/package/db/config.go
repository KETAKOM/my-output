package db

import (
	"fmt"
	"os"
)

// 環境変数からDBの接続情報を取得する
func GetDBConfig() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
	)
}

// 環境変数からDBの接続情報を取得する
func GetDBConfigV2() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		os.Getenv("DB_USER_V2"),
		os.Getenv("DB_PASSWORD_V2"),
		os.Getenv("DB_HOST_V2"),
		os.Getenv("DB_PORT_V2"),
		os.Getenv("DB_NAME_V2"),
	)
}
