package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"slices"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/topotal/cloudwatch-logs-s3-exporter/internal/state"
)

const TypeS3 StoreType = "s3"

type S3Store struct {
	ctx    context.Context
	client *s3.Client
	bucket string
	key    string
	states state.States
	logger *slog.Logger
}

func NewS3(ctx context.Context, sc *Config) (*S3Store, error) {
	cfg, err := awscfg.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg)

	dsn, err := url.Parse(sc.DSN)
	if err != nil {
		return nil, err
	}

	if dsn.Scheme != "s3" {
		return nil, errors.New("invalid DSN scheme. expected s3://")
	}

	if dsn.Host == "" {
		return nil, errors.New("missing s3 bucket name")
	}

	if dsn.Path == "" {
		return nil, errors.New("missing s3 key")
	}
	// Remove the leading slash
	dsn.Path = dsn.Path[1:]

	return &S3Store{
		ctx:    ctx,
		client: client,
		bucket: dsn.Host,
		key:    dsn.Path,
		logger: sc.Logger,
	}, nil
}

func (s *S3Store) Initialize() error {
	var states state.States

	out, err := s.client.GetObject(
		s.ctx,
		&s3.GetObjectInput{
			Bucket: &s.bucket,
			Key:    &s.key,
		},
	)

	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			s.logger.Info("state not found", slog.String("bucket", s.bucket), slog.String("key", s.key))
			s.states = make(state.States, 0)
			return nil
		}
		return err
	}

	if err := json.NewDecoder(out.Body).Decode(&states); err != nil {
		return err
	}

	s.states = states

	return nil
}

func (s *S3Store) PutState(st state.State) error {
	// Only update the state in memory to reduce the number of PutObject calls
	idx := slices.IndexFunc(s.states, func(r *state.State) bool {
		return r.LogStreamArn == st.LogStreamArn
	})

	if idx == -1 {
		s.states = append(s.states, &st)
	} else {
		s.states[idx] = &st
	}

	return nil
}

func (s *S3Store) GetState(arn string) (st *state.State, err error) {
	idx := slices.IndexFunc(s.states, func(r *state.State) bool {
		return r.LogStreamArn == arn
	})

	if idx == -1 {
		return nil, nil
	}

	return s.states[idx], nil
}

func (s *S3Store) Finalize() error {
	body, err := json.Marshal(s.states)
	if err != nil {
		return err
	}

	_, err = s.client.PutObject(
		s.ctx,
		&s3.PutObjectInput{
			Bucket: &s.bucket,
			Key:    &s.key,
			Body:   bytes.NewReader(body),
		},
	)

	if err != nil {
		return err
	}

	s.logger.Info("state stored", slog.String("bucket", s.bucket), slog.String("key", s.key), slog.Int("size", len(body)))

	return nil
}
