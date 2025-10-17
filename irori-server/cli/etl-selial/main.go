package main

import (
	"context"
	"database/sql"
	"fmt"
	"irori-server/package/db"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Record struct {
	Date       time.Time
	Timestamp  time.Time
	DeviceType string
	EventType  string
	Message    string
}

type Records []Record

// ====== Extract ======
func Extractor(ctx context.Context, db *sql.DB, chunkSize, offset int) (Records, error) {
	query := fmt.Sprintf(`
		SELECT date, timestamp, device_type, event_type, message
		FROM raw_records
		Where timestamp BETWEEN '2025-10-17 00:00:00' AND '2025-10-17 23:59:59'
		ORDER BY id
		LIMIT %d OFFSET %d`, chunkSize, offset)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	var records Records
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.Date, &r.Timestamp, &r.DeviceType, &r.EventType, &r.Message); err != nil {
			return records, fmt.Errorf("scan error: %w", err)
		}
		records = append(records, r)
	}
	return records, nil
}

// ====== Transform ======
func Transformer(ctx context.Context, in Records) (Records, error) {
	for i := range in {
		in[i].Message = strings.ToUpper(in[i].Message)
	}
	return in, nil
}

// ====== Load ======
func Loader(ctx context.Context, db *sql.DB, records Records) error {
	if len(records) == 0 {
		return nil
	}

	queryPrefix := "INSERT INTO records (date, timestamp, device_type, event_type, message) VALUES "
	values := make([]string, 0, len(records))

	for _, r := range records {
		v := fmt.Sprintf("('%s', '%s', '%s', '%s', '%s')",
			r.Date.Format("2006-01-02"),
			r.Timestamp.Format("2006-01-02 15:04:05"),
			r.DeviceType, r.EventType, r.Message)
		values = append(values, v)
	}

	query := queryPrefix + strings.Join(values, ",")
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("insert error: %w", err)
	}
	return nil
}

// ====== Main ETL ======
func main() {
	ctx := context.Background()

	connection := db.NewDBConnection()
	sourceDB, err := connection.OpenDB()
	if err != nil {
		log.Fatal(err)
	}
	sourceDB.SetMaxOpenConns(8)                  // 同時接続上限
	sourceDB.SetMaxIdleConns(3)                  // アイドル保持
	sourceDB.SetConnMaxLifetime(3 * time.Minute) // 再利用期限（Proxy切断対策）

	targetDB, err := connection.OpenDBV2()
	if err != nil {
		log.Fatal(err)
	}
	targetDB.SetMaxOpenConns(8)                  // 同時接続上限
	targetDB.SetMaxIdleConns(3)                  // アイドル保持
	targetDB.SetConnMaxLifetime(3 * time.Minute) // 再利用期限（Proxy切断対策）

	defer func() {
		sourceDB.Close()
		targetDB.Close()
	}()

	const chunkSize = 5000

	offset := 0
	for {
		// Extract
		records, err := Extractor(ctx, sourceDB, chunkSize, offset)
		if err != nil {
			log.Fatalf("extract error: %v", err)
		}
		if len(records) == 0 {
			log.Println("ETL completed successfully.")
			break
		}

		// Transform
		transformed, err := Transformer(ctx, records)
		if err != nil {
			log.Fatalf("transform error: %v", err)
		}

		// Load
		if err := Loader(ctx, targetDB, transformed); err != nil {
			log.Fatalf("load error: %v", err)
		}

		offset += chunkSize
		log.Printf("Processed chunk: offset=%d, count=%d\n", offset, len(records))
	}
}
