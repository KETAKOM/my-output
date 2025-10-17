package main

import (
	"context"
	"database/sql"
	"fmt"
	"irori-server/package/db"
	"log"
	"math/rand"
	"strings"
	"time"
)

func main() {
	connection := db.NewDBConnection()
	db, err := connection.OpenDB()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		db.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	insertRecordData(ctx, db)
}

func insertRecordData(ctx context.Context, db *sql.DB) {
	// === データ生成とバルクINSERT ===
	baseDateStr := "2025-10-14"
	total := 5000000   // 総件数（変更可）
	batchSize := 10000 // バルクサイズ
	start := time.Now()
	rand.Seed(time.Now().UnixNano())

	layout := "2006-01-02" // Goの日時フォーマット定数
	baseDate, err := time.Parse(layout, baseDateStr)
	if err != nil {
		panic(err)
	}

	// ====== テストデータ生成 ======
	deviceTypes := []string{"sensor-a-", "sensor-b-", "sensor-c-"}
	eventTypes := []string{"event", "info", "error"}
	messages := []string{"info", "success", "error"}

	for i := 0; i < total; i += batchSize {
		// VALUES句の部分を生成
		valueStrings := make([]string, 0, batchSize)
		valueArgs := make([]interface{}, 0, batchSize*5)

		for j := 0; j < batchSize; j++ {
			date := baseDateStr
			timestamp := baseDate.Add(time.Duration(rand.Intn(10000)) * time.Second)
			deviceType := deviceTypes[rand.Intn(len(deviceTypes))] + randomString(10)
			eventType := eventTypes[rand.Intn(len(eventTypes))]
			message := messages[rand.Intn(len(messages))]

			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, date, timestamp, deviceType, eventType, message)
		}

		query := fmt.Sprintf(`
		INSERT INTO raw_records (date, timestamp, device_type, event_type, message)
		VALUES %s
		ON DUPLICATE KEY UPDATE
			message = VALUES(message)
	`, strings.Join(valueStrings, ","))

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			log.Println("begin tx error: %w", err)
		}

		if _, err := tx.ExecContext(ctx, query, valueArgs...); err != nil {
			tx.Rollback()
			log.Println("bulk upsert error: %w", err)
		}
		if err := tx.Commit(); err != nil {
			log.Println("commit error: %w", err)
		}

		if i%100000 == 0 {
			log.Printf("Inserted %d rows...", i)
		}
	}

	log.Printf("🎉 Done! Inserted %d rows in %v", total, time.Since(start))
}

// ランダムな英数字10文字を生成
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	bytes := make([]byte, n)
	for i := range bytes {
		bytes[i] = letters[rand.Intn(len(letters))]
	}
	return string(bytes)
}
