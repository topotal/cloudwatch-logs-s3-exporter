package exporter

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/topotal/cloudwatch-logs-s3-exporter/internal/cwlogs"
)

type Target struct {
	LogGroup   types.LogGroup    `json:"logGroup"`
	LogStreams []types.LogStream `json:"logStreams"`
}

func NewTargets(ctx context.Context, client *cwlogs.Client, groupPrefixes []string) (targets []Target, err error) {
	groups, err := client.DescribeLogGroupsFromPrefixies(ctx, groupPrefixes)
	if err != nil {
		return nil, err
	}

	for _, group := range groups {
		streams, err := client.DescribeLogStreamsFromGroup(ctx, group)
		if err != nil {
			return nil, err
		}
		targets = append(targets, Target{
			LogGroup:   group,
			LogStreams: streams,
		})
	}

	return targets, nil
}
