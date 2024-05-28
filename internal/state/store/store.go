package store

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/topotal/cloudwatch-logs-s3-exporter/internal/state"
)

type StoreType string

type Store interface {
	Initialize() error
	GetState(arn string) (*state.State, error)
	PutState(state state.State) error
	Finalize() error
}

type Config struct {
	DSN    string
	Logger *slog.Logger
}

func NewStore(ctx context.Context, logger *slog.Logger, stype StoreType, dsn string) (Store, error) {
	config := &Config{
		DSN:    dsn,
		Logger: logger,
	}

	switch strings.ToLower(string(stype)) {
	case "s3":
		return NewS3(ctx, config)
	case "dynamodb":
		return NewDynamoDBStore(ctx, config)
	default:
		return nil, errors.New("unsupported store type")
	}
}
