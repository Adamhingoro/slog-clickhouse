package slogclickhouse

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	slogmulti "github.com/samber/slog-multi"
)

func TestClickHouseHandler(t *testing.T) {
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{"localhost:9001"},
		Auth: clickhouse.Auth{
			Database: "logging",
			Username: "",
			Password: "",
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:          time.Second * 30,
		Debug:                true,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
	})

	if err := conn.Ping(); err != nil {
		t.Log("local clickhouse server is not running, skipping test...")
		return
	}

	conn.SetMaxIdleConns(5)
	conn.SetMaxOpenConns(10)
	conn.SetConnMaxLifetime(time.Hour)

	chHandler := Option{
		Level:     slog.LevelWarn,
		DB:        conn,
		LogTable:  "logging.logs",
		Namespace: "Dummy-Namespace",
		Service:   "logging",
	}.NewClickHouseHandler()

	handler := slogmulti.Fanout(
		chHandler, // pass to first handler: save warn and error logs to clickhouse
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}), // then to second handler: print all info and above logs to stdout
	)

	logger := slog.New(handler)

	logger.Error("Hello, ClickHouse!", "key1", "value1", "key2", 2)
	logger.Error("sample request id", "uid", 123456, "request_id", "ABC123")
	logger.Error("sample request id", "uid", 123456, "request_id", "ABC100", "some_key", "somevalue")
	logger.Error("sample request id", "uid", 123456, "request_id", "ABC100", "some_int", 1234)

}
