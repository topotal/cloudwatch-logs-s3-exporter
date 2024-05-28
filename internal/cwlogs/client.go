package cwlogs

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

// CloudWatch Logs Client Wrapper
type Client struct {
	client *cloudwatchlogs.Client
}

func NewClient(ctx context.Context) (*Client, error) {
	config, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := cloudwatchlogs.NewFromConfig(config)

	return &Client{
		client: client,
	}, nil
}

func (c *Client) DescribeLogGroupsFromPrefixies(ctx context.Context, prefixes []string) (groups []types.LogGroup, err error) {
	for _, prefix := range prefixes {
		result, err := c.DescribeLogGroupsWithPagination(
			ctx,
			&cloudwatchlogs.DescribeLogGroupsInput{
				LogGroupNamePrefix: &prefix,
			},
		)
		if err != nil {
			return nil, err
		}

		groups = append(groups, result...)
	}

	return groups, nil
}

func (c *Client) DescribeLogStreamsFromGroup(ctx context.Context, group types.LogGroup) (streams []types.LogStream, err error) {
	result, err := c.DescribeLogStreamsWithPagination(
		ctx,
		&cloudwatchlogs.DescribeLogStreamsInput{
			LogGroupName: group.LogGroupName,
			OrderBy:      types.OrderByLastEventTime,
		},
	)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (c *Client) DescribeLogGroupsWithPagination(ctx context.Context, input *cloudwatchlogs.DescribeLogGroupsInput) (groups []types.LogGroup, err error) {
	p := cloudwatchlogs.NewDescribeLogGroupsPaginator(c.client, input)
	for p.HasMorePages() {
		output, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		groups = append(groups, output.LogGroups...)
	}

	return groups, nil
}

func (c *Client) DescribeLogStreamsWithPagination(ctx context.Context, input *cloudwatchlogs.DescribeLogStreamsInput) (streams []types.LogStream, err error) {
	p := cloudwatchlogs.NewDescribeLogStreamsPaginator(c.client, input)
	for p.HasMorePages() {
		output, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		streams = append(streams, output.LogStreams...)
	}

	return streams, nil
}

func (c *Client) CreateExportTask(ctx context.Context, input *cloudwatchlogs.CreateExportTaskInput) (id string, err error) {
	output, err := c.client.CreateExportTask(ctx, input)
	if err != nil {
		return "", err
	}

	return *output.TaskId, nil
}

func (c *Client) IsExported(ctx context.Context, id string) (bool, error) {
	result, err := c.client.DescribeExportTasks(ctx, &cloudwatchlogs.DescribeExportTasksInput{
		TaskId: &id,
	})
	if err != nil {
		return false, err
	}

	if len(result.ExportTasks) == 0 {
		return false, errors.New("export task not found")
	}

	task := result.ExportTasks[0]
	if task.Status.Code == types.ExportTaskStatusCodeCompleted {
		return true, nil
	}

	return false, nil
}
