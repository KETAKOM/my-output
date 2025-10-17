package etl

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"time"
)

// ====== Extract ======
func Extractor(
	ctx context.Context,
	db *sql.DB,
	out chan<- Record,
	chunkSize int,
	workerID int,

	minID, maxID int64,
	startAt, endAt time.Time,
) error {
	lastID := minID - 1
	chunkCount := 0

	startStr := startAt.Format("2006-01-02 15:04:05")
	endStr := endAt.Format("2006-01-02 15:04:05")

	for {
		query := `
			SELECT id, date, timestamp, device_type, event_type, message
			FROM raw_records
			WHERE id > ? AND id <= ?
			AND timestamp >= ? AND timestamp < ?
			ORDER BY id
			LIMIT ?;`

		rows, err := db.QueryContext(ctx, query, strconv.FormatInt(lastID, 10), strconv.FormatInt(maxID, 10), startStr, endStr, chunkSize)
		if err != nil {
			return fmt.Errorf("query error: %w", err)
		}

		log.Printf("[Worker %d][Chunk %d] last: %s maxID: %s", workerID, chunkCount, strconv.FormatInt(lastID, 10), strconv.FormatInt(maxID, 10))

		count := 0
		var maxFetchedID int64
		for rows.Next() {
			var r Record
			if err := rows.Scan(&r.ID, &r.Date, &r.Timestamp, &r.DeviceType, &r.EventType, &r.Message); err != nil {
				rows.Close()
				return fmt.Errorf("scan error: %w", err)
			}
			select {
			case <-ctx.Done():
				rows.Close()
				return ctx.Err()
			default:
				out <- r
				count++
				maxFetchedID = r.ID
			}
			count++
		}
		rows.Close()

		if count == 0 {
			// 最後まで到達
			break
		}

		lastID = maxFetchedID
		chunkCount++
	}
	return nil
}
