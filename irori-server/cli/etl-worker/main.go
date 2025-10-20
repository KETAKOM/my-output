package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"irori-server/cli/etl-worker/etl"
	"log"
	"sync"
	"time"
)

const (
	extractWorker    = 4
	transformWorker  = 4
	loadWorker       = 2
	chunkSize        = 5000
	bulkSize         = 10000
	extractChCount   = 5000
	transformChCount = 5000
)

// getIDRange は、テーブル内の最小IDと最大IDを取得します
func getIDRange(ctx context.Context, db *sql.DB, startAt, endAt time.Time) (minID, maxID int64, err error) {
	query := `
		SELECT MIN(id), MAX(id)
		FROM raw_records
		WHERE timestamp >= ? AND timestamp < ?;
	`

	starStr := startAt.Format("2006-01-02 15:04:05")
	endStr := endAt.Format("2006-01-02 15:04:05")

	row := db.QueryRowContext(ctx, query, starStr, endStr)
	if err := row.Scan(&minID, &maxID); err != nil {
		return 0, 0, fmt.Errorf("failed to get ID range: %w", err)
	}
	return minID, maxID, nil
}

// parseDateRange
// 開始日時と終了日時の取得
// 引数指定がない場合は、前日の0時０分0秒~今日の0時０分0秒までを範囲とする
func parseDateRange(startAtStr, endAtStr string) (time.Time, time.Time, error) {
	const layout = "2006-01-02 15:04:05"

	var startAt, endAt time.Time
	var err error

	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	yesterday := today.AddDate(0, 0, -1)

	// --- どちらも未指定 ---
	if startAtStr == "" && endAtStr == "" {
		startAt = yesterday
		endAt = today
		return startAt, endAt, nil
	}

	// --- 片方のみ指定された場合 ---
	if startAtStr == "" || endAtStr == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("validation error: invalid argument")
	}

	startAt, err = time.ParseInLocation(layout, startAtStr, time.Local)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid start_at format: %v", err)
	}
	endAt, err = time.ParseInLocation(layout, endAtStr, time.Local)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid end_at format: %v", err)
	}

	// --- バリデーション ---
	if endAt.Before(startAt) {
		return time.Time{}, time.Time{}, fmt.Errorf("validation error: endAt (%s) is before startAt (%s)", endAt.Format(layout), startAt.Format(layout))
	}

	return startAt, endAt, nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// timestampの範囲を指定
	startAtArgs := flag.String("start_at", "", "Start datetime (format: yyyy-mm-dd hh:mm:ss)")
	endAtArgs := flag.String("end_at", "", "End datetime (format: yyyy-mm-dd hh:mm:ss)")
	flag.Parse()
	startAt, endAt, err := parseDateRange(*startAtArgs, *endAtArgs)
	if err != nil {
		log.Fatal("parseDateRange Error: ", err)
		cancel()
	}

	sourceDB, targetDB, err := etl.GetDBConnections()
	if err != nil {
		log.Fatal("GetDBConnections Error: ", err)
		cancel()
	}
	defer func() {
		sourceDB.Close()
		targetDB.Close()
	}()

	// 対象となるID範囲を取得し、均等に分割
	minID, maxID, err := getIDRange(ctx, sourceDB, startAt, endAt)
	if err != nil {
		log.Fatal("GetIDRange Error: ", err)
		cancel()
	}
	rangeSize := (maxID - minID + 1) / int64(extractWorker)

	extractCh := make(chan etl.Record, extractChCount)
	transformCh := make(chan etl.Record, transformChCount)
	var wgE, wgT, wgL sync.WaitGroup

	// --- Extract (並行) ---
	for i := 0; i < extractWorker; i++ {
		rangeStart := minID + int64(i)*rangeSize
		rangeEnd := rangeStart + rangeSize - 1

		wgE.Add(1)
		go func(workerID, offset int) {
			defer wgE.Done()
			if err := etl.Extractor(ctx, sourceDB, extractCh, chunkSize, workerID, rangeStart, rangeEnd, startAt, endAt); err != nil {
				log.Printf("extract(w%d): %v", workerID, err)
				cancel()
			}
		}(i, i*chunkSize)
	}

	// --- Transform (並行) ---
	for i := 0; i < transformWorker; i++ {
		wgT.Add(1)
		go func(id int) {
			defer wgT.Done()
			etl.Transformer(ctx, extractCh, transformCh)
		}(i)
	}

	// --- Load (並行) ---
	for i := 0; i < loadWorker; i++ {
		wgL.Add(1)
		go func(id int) {
			defer wgL.Done()
			etl.LoaderBulkUpsert(ctx, targetDB, transformCh, bulkSize, id)
		}(i)
	}

	wgE.Wait()
	close(extractCh)
	wgT.Wait()
	close(transformCh)
	wgL.Wait()

	log.Println("ETLが完了しました")
}
