DROP TABLE IF EXISTS records;

CREATE TABLE records (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL COMMENT '日付',
    timestamp DATETIME NOT NULL COMMENT 'イベントの発生時刻',
    device_type VARCHAR(255) NOT NULL COMMENT 'デバイスタイプ',
    event_type VARCHAR(255) NOT NULL COMMENT 'イベントタイプ',
    message TEXT COMMENT 'メッセージ内容',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '作成日時',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新日時',
    UNIQUE KEY uniq_device_event_time (device_type, event_type, timestamp),
    INDEX idx_timestamp (timestamp),
    INDEX idx_device_event_time (device_type, event_type, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
