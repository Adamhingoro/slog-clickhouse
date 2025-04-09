package slogclickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"log/slog"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	slogcommon "github.com/samber/slog-common"
)

type Option struct {
	// Hostname, optional: os.Hostname() will be used if not set
	Hostname string

	// Few additional attributes for logging.
	Namespace string
	Service   string

	// log level (default: debug)
	Level slog.Leveler

	// ClickHouse Connection
	DB *sql.DB
	// ClickHouse Log Table
	LogTable string
	Timeout  time.Duration // default: 60s

	// optional: customize clickhouse event builder
	Converter Converter

	// optional: see slog.HandlerOptions
	AddSource   bool
	ReplaceAttr func(groups []string, a slog.Attr) slog.Attr
}

func (o Option) NewClickHouseHandler() slog.Handler {
	if o.Hostname == "" {
		hostname, err := os.Hostname()
		if err != nil {
			panic("missing hostname")
		}
		o.Hostname = hostname
	}

	if o.Level == nil {
		o.Level = slog.LevelDebug
	}

	if o.DB == nil {
		panic("missing clickhouse db connection")
	}

	if o.Namespace == "" {
		panic("missing namespace for logging")
	}

	if o.Service == "" {
		panic("missing service name for logging")
	}

	if o.LogTable == "" {
		panic("missing log table name")
	}

	if o.Timeout == 0 {
		o.Timeout = 60 * time.Second
	}

	if o.Converter == nil {
		o.Converter = DefaultConverter
	}

	return &ClickHouseHandler{
		option: o,
		attrs:  []slog.Attr{},
		groups: []string{},
	}
}

var _ slog.Handler = (*ClickHouseHandler)(nil)

type ClickHouseHandler struct {
	option Option
	attrs  []slog.Attr
	groups []string
}

func (h *ClickHouseHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.option.Level.Level()
}

func (h *ClickHouseHandler) Handle(ctx context.Context, record slog.Record) error {
	payload := h.option.Converter(h.option.AddSource, h.option.ReplaceAttr, h.attrs, h.groups, &record)

	return h.saveToDB(record.Time, record, payload)
}

func (h *ClickHouseHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ClickHouseHandler{
		option: h.option,
		attrs:  slogcommon.AppendAttrsToGroup(h.groups, h.attrs, attrs...),
		groups: h.groups,
	}
}

func (h *ClickHouseHandler) WithGroup(name string) slog.Handler {
	return &ClickHouseHandler{
		option: h.option,
		attrs:  h.attrs,
		groups: append(h.groups, name),
	}
}

func buildMapLiteral(m map[string]string) string {
	var pairs []string
	for key, value := range m {
		// Use single quotes and properly join the key-value pairs.
		pairs = append(pairs, fmt.Sprintf("'%s', '%s'", key, value))
	}
	return fmt.Sprintf("map(%s)", strings.Join(pairs, ", "))
}

func valueToString(value any) string {
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", value)
}

func (h *ClickHouseHandler) saveToDB(timestamp time.Time, record slog.Record, payload map[string]any) error {
	level := record.Level.String()
	message := record.Message
	var uid string
	var request_id string

	// Convert payload (map[string]any) to a native map[string]string.
	attrs := make(map[string]string, len(payload))
	for key, value := range payload {
		lowerKey := strings.ToLower(key)
		if lowerKey == "uid" || lowerKey == "user_id" || lowerKey == "userid" {
			uid = valueToString(value)
			continue
		}

		if lowerKey == "rid" || lowerKey == "request_id" || lowerKey == "requestid" {
			request_id = valueToString(value)
			continue
		}

		if s, ok := value.(string); ok {
			attrs[key] = s
		} else {
			attrs[key] = fmt.Sprintf("%v", value)
		}
	}

	// Build the ClickHouse map literal for the attributes column.
	attrLiteral := buildMapLiteral(attrs)

	// Construct the SQL string by inlining the map literal.
	// The first six columns are passed as parameters.
	sql := fmt.Sprintf("INSERT INTO %s (timestamp, hostname, namespace, service, level, message, attributes, uid, request_id) VALUES (?, ?, ?, ?, ?, ?, %s, ?, ?)",
		h.option.LogTable, attrLiteral)

	// Execute the query.
	_, err := h.option.DB.Exec(sql,
		timestamp,
		h.option.Hostname,
		h.option.Namespace,
		h.option.Service,
		level,
		message,
		uid,
		request_id)
	return err
}
