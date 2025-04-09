package main

import (
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	//"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2"
	slogclickhouse "github.com/smallnest/slog-clickhouse"
)

var logLevels = []slog.Level{
	slog.LevelDebug,
	slog.LevelInfo,
	slog.LevelWarn,
	slog.LevelError,
}

var services = []string{"Auth", "Message", "Media", "Billing", "Notification"}
var eventNames = []string{"LoginAttempt", "FileUpload", "SendMessage", "Subscribe", "Logout", "PaymentProcessed"}

func randomLogLevel() slog.Level {
	return logLevels[rand.Intn(len(logLevels))]
}

func randomService() string {
	return services[rand.Intn(len(services))]
}

func randomEventName() string {
	return eventNames[rand.Intn(len(eventNames))]
}

func randomUserID() int {
	return rand.Intn(10000)
}

func randomTokenID() string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 16)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func main() {

	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9001"},
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
		fmt.Println("local clickhouse server is not running, skipping test...")
		return
	}

	conn.SetMaxIdleConns(5)
	conn.SetMaxOpenConns(10)
	conn.SetConnMaxLifetime(time.Hour)

	logger := slog.New(slogclickhouse.Option{Level: slog.LevelDebug, DB: conn, LogTable: "logs"}.NewClickHouseHandler())

	rand.Seed(time.Now().UnixNano())

	for {
		level := randomLogLevel()
		service := randomService()
		userID := randomUserID()
		tokenID := randomTokenID()
		eventName := randomEventName()

		msg := "Generated random log"

		logAttrs := []any{
			slog.String("service", service),
			slog.Int("userId", userID),
			slog.String("tokenId", tokenID),
			slog.String("eventName", eventName),
		}

		switch level {
		case slog.LevelDebug:
			logger.Debug(msg, logAttrs...)
		case slog.LevelInfo:
			logger.Info(msg, logAttrs...)
		case slog.LevelWarn:
			logger.Warn(msg, logAttrs...)
		case slog.LevelError:
			logger.Error(msg, logAttrs...)
		}

		time.Sleep(2 * time.Second)
	}
}
