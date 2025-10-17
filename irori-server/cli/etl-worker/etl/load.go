package etl

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
)

// ====== Load (Bulk UPSERT) ======

func LoaderBulkUpsert(ctx context.Context, db *sql.DB, in <-chan Record, bulkSize int, workerID int) {
	buffer := make([]Record, 0, bulkSize)

	flush := func(recs []Record) error {
		log.Println("Load: ", workerID)
		if len(recs) == 0 {
			return nil
		}

		values := make([]string, 0, len(recs))
		args := make([]interface{}, 0, len(recs)*5)
		for _, r := range recs {
			values = append(values, "(?, ?, ?, ?, ?)")
			args = append(args, r.Date, r.Timestamp, r.DeviceType, r.EventType, r.Message)
		}

		query := fmt.Sprintf(`
			INSERT INTO records (date, timestamp, device_type, event_type, message)
			VALUES %s
			ON DUPLICATE KEY UPDATE
				message = VALUES(message)
		`, strings.Join(values, ","))

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx error: %w", err)
		}

		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			tx.Rollback()
			return fmt.Errorf("bulk upsert error: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit error: %w", err)
		}
		return nil
	}

	defer func() {
		// 最後の残りをflush
		if err := flush(buffer); err != nil {
			log.Printf("final flush error: %v", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case r, ok := <-in:
			if !ok {
				return
			}
			buffer = append(buffer, r)
			if len(buffer) >= bulkSize {
				if err := flush(buffer); err != nil {
					log.Printf("flush error: %v", err)
				}
				buffer = buffer[:0]
			}
		}
	}
}
