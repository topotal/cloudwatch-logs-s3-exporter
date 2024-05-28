package exporter

import (
	"context"
	"fmt"
	"log/slog"
	"path"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/topotal/cloudwatch-logs-s3-exporter/internal/cwlogs"
	"github.com/topotal/cloudwatch-logs-s3-exporter/internal/state"
	"github.com/topotal/cloudwatch-logs-s3-exporter/internal/state/store"
)

type Exporter struct {
	ctx     context.Context
	bucket  string
	targets []Target
	store   store.Store
	client  *cwlogs.Client
	logger  *slog.Logger
}

func NewExporter(ctx context.Context, logger *slog.Logger, client *cwlogs.Client, bucket string, targets []Target, store store.Store) Exporter {
	return Exporter{
		ctx:     ctx,
		bucket:  bucket,
		targets: targets,
		store:   store,
		client:  client,
		logger:  logger,
	}
}

func (e *Exporter) Export(finalizeDuration time.Duration) error {
	now := time.Now()
	deadlineReached := false

	for _, target := range e.targets {
		for _, stream := range target.LogStreams {
			st, err := e.store.GetState(*stream.Arn)
			if err != nil {
				return err
			}

			if target.LogGroup.RetentionInDays != nil && *target.LogGroup.RetentionInDays > 0 {
				retention := time.Duration(*target.LogGroup.RetentionInDays) * 24 * time.Hour
				if now.Sub(time.UnixMilli(*stream.LastIngestionTime)) > retention {
					e.logger.Info("skipping export: all stream contents are beyond their retention period", "logStreamArn", *stream.Arn, "logStreamName", *stream.LogStreamName, "retentionInDays", *target.LogGroup.RetentionInDays)
					continue
				}
			}

			from := e.exportFrom(st, stream)
			if from == nil {
				// already exported or not ready to export
				e.logger.Info("skipping export: data has already been exported", "logStreamArn", *stream.Arn, "logStreamName", *stream.LogStreamName)
				continue
			}

			e.logger.Info("exporting", "logStreamArn", *stream.Arn, "logStreamName", *stream.LogStreamName, "from", from, "to", stream.LastIngestionTime)
			id, err := e.client.CreateExportTask(e.ctx, &cloudwatchlogs.CreateExportTaskInput{
				Destination:         &e.bucket,
				LogGroupName:        target.LogGroup.LogGroupName,
				LogStreamNamePrefix: stream.LogStreamName,
				From:                from,
				To:                  stream.LastIngestionTime,
				DestinationPrefix:   aws.String(e.destinationPrefix(now, *target.LogGroup.LogGroupName, *stream.LogStreamName)),
			})
			if err != nil {
				return err
			}

			// Due to the limitation of the CloudWatch Logs API, we need to wait for the export task to be completed
			// refs: https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
			deadline, _ := e.ctx.Deadline()
			for {
				exported, err := e.client.IsExported(e.ctx, id)
				if err != nil {
					return err
				}

				if exported {
					break
				}

				if time.Now().After(deadline.Add(-finalizeDuration)) {
					e.logger.Warn("the export task is not completed, but the process is being finalized", "taskID", id)
					deadlineReached = true
					break
				}

				<-time.After(5 * time.Second)
			}

			new := state.State{
				LogGroupArn:  *target.LogGroup.Arn,
				LogStreamArn: *stream.Arn,
				ExportedAt:   now.UnixMilli(),
				TaskId:       id,
			}

			if err := e.store.PutState(new); err != nil {
				return err
			}

			if deadlineReached {
				return nil
			}
		}
	}

	return nil
}

func (e *Exporter) destinationPrefix(now time.Time, group, stream string) string {
	return path.Join(
		fmt.Sprintf("%04d", now.Year()),
		fmt.Sprintf("%02d", now.Month()),
		fmt.Sprintf("%02d", now.Day()),
		group,
		stream,
	)
}

func (e *Exporter) exportFrom(st *state.State, stream types.LogStream) *int64 {
	switch {
	case st == nil:
		// export from the creation time
		return stream.CreationTime
	case st.ExportedAt >= *stream.LastIngestionTime:
		// already exported
		return nil
	case st.ExportedAt < *stream.LastIngestionTime:
		// resume from the last exported time
		return &st.ExportedAt
	default:
		// export from the creation time
		return stream.CreationTime
	}
}
