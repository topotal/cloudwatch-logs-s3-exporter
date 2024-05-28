package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"

	"github.com/topotal/cloudwatch-logs-s3-exporter/internal/cwlogs"
	"github.com/topotal/cloudwatch-logs-s3-exporter/internal/exporter"
	"github.com/topotal/cloudwatch-logs-s3-exporter/internal/state/store"
)

const (
	FinalizeDuration = 30 * time.Second
)

type Event struct {
	DestinationS3Bucket    string `json:"destinationS3Bucket"`
	SourceLogGroupPrefixes string `json:"sourceLogGroupPrefixes"`
	StateStoreType         string `json:"stateStoreType"`
	StoreDSN               string `json:"storeDSN"`
	LogLevel               string `json:"logLevel"`
}

type Response struct {
	Targets []exporter.Target `json:"targets"`
	Errors  []string          `json:"messages"`
}

var LogLevelMap = map[string]slog.Level{
	"DEBUG": slog.LevelDebug,
	"INFO":  slog.LevelInfo,
	"WARN":  slog.LevelWarn,
	"ERROR": slog.LevelError,
}

func main() {
	lambda.StartWithOptions(HandleRequest)
}

func HandleRequest(ctx context.Context, event Event) (resp Response, err error) {
	level, ok := LogLevelMap[event.LogLevel]
	if !ok {
		level = slog.LevelInfo
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	client, err := cwlogs.NewClient(ctx)
	if err != nil {
		return BuildResponseOnError(logger, resp, err)
	}

	store, err := store.NewStore(ctx, logger, store.StoreType(event.StateStoreType), event.StoreDSN)
	if err != nil {
		return BuildResponseOnError(logger, resp, err)
	}

	if err := store.Initialize(); err != nil {
		return BuildResponseOnError(logger, resp, err)
	}

	prefixes := strings.Split(event.SourceLogGroupPrefixes, ",")
	targets, err := exporter.NewTargets(ctx, client, prefixes)
	if err != nil {
		return BuildResponseOnError(logger, resp, err)
	}
	resp.Targets = targets

	exporter := exporter.NewExporter(ctx, logger, client, event.DestinationS3Bucket, targets, store)

	if err := exporter.Export(FinalizeDuration); err != nil {
		return BuildResponseOnError(logger, resp, err)
	}

	if err := store.Finalize(); err != nil {
		return BuildResponseOnError(logger, resp, err)
	}

	return resp, nil
}

func BuildResponseOnError(logger *slog.Logger, resp Response, err error) (Response, error) {
	logger.Error(err.Error())
	resp.Errors = append(resp.Errors, err.Error())
	return resp, err
}
