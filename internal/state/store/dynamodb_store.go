package store

import (
	"context"
	"errors"
	"net/url"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/topotal/cloudwatch-logs-s3-exporter/internal/state"
)

const TypeDynamoDB StoreType = "dynamodb"

type DynamoDBStore struct {
	ctx    context.Context
	client *dynamodb.Client
	table  string
}

func NewDynamoDBStore(ctx context.Context, sc *Config) (*DynamoDBStore, error) {
	cfg, err := awscfg.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := dynamodb.NewFromConfig(cfg)

	dsn, err := url.Parse(sc.DSN)
	if err != nil {
		return nil, err
	}

	return &DynamoDBStore{
		ctx:    ctx,
		client: client,
		table:  dsn.Host,
	}, nil
}

func (s *DynamoDBStore) GetState(arn string) (*state.State, error) {
	return nil, errors.New("not implemented")
}

func (s *DynamoDBStore) PutState(state.State) error {
	return errors.New("not implemented")
}

func (s *DynamoDBStore) Initialize() error {
	return nil
}

func (s *DynamoDBStore) Finalize() error {
	return nil
}
