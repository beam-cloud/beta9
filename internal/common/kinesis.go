package common

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	beta9Types "github.com/beam-cloud/beta9/internal/types"
)

var shardRecordLimit int32 = 100

type KinesisClient struct {
	client             *kinesis.Client
	ctx                *context.Context
	shardIterators     map[string]*string
	shardIteratorMutex sync.Mutex
	sinks              map[string]*beta9Types.EventSink
	sinkMutex          sync.Mutex
	streamName         string
}

type KinesisClientOption func(*kinesis.Options)

// WithEndpoint allows the consumer to set a custom kinesis endpoint URL (currently only used for LocalStack Kinesis)
func WithKinesisEndpoint(url string) KinesisClientOption {
	return func(opts *kinesis.Options) {
		opts.BaseEndpoint = aws.String(url)
	}
}

func WithCredentials(key, secret, session string) KinesisClientOption {
	return func(opts *kinesis.Options) {
		opts.Credentials = aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(key, secret, session))
	}
}

func NewKinesisClient(ctx context.Context, streamName string, region string, opts ...KinesisClientOption) (*KinesisClient, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, err
	}

	kinesisOpts := make([]func(*kinesis.Options), 0)
	for _, opt := range opts {
		kinesisOpts = append(kinesisOpts, opt)
	}

	client := kinesis.NewFromConfig(cfg, kinesisOpts...)
	return &KinesisClient{
		client:     client,
		ctx:        &ctx,
		sinks:      make(map[string]*beta9Types.EventSink),
		streamName: streamName,
	}, nil
}

func (kcc *KinesisClient) ListenToStream() {
	go kcc.startShardDiscovery(kcc.streamName)
	kcc.IterateShards()
}

func (kcc *KinesisClient) startShardDiscovery(streamName string) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	kcc.discoverShard(streamName) // Initial discovery

	for {
		select {
		case <-(*kcc.ctx).Done():
			return
		case <-ticker.C:
			kcc.discoverShard(streamName)
		}
	}
}

func (kcc *KinesisClient) discoverShard(streamName string) {
	describeStreamResp, err := kcc.client.DescribeStream(context.TODO(), &kinesis.DescribeStreamInput{
		StreamName: &streamName,
	})
	if err != nil {
		log.Fatalf("Failed to describe stream, %v", err)
	}

	shardIterators := make(map[string]*string)

	for _, shard := range describeStreamResp.StreamDescription.Shards {
		shardIteratorResp, err := kcc.client.GetShardIterator(context.TODO(), &kinesis.GetShardIteratorInput{
			ShardId:           shard.ShardId,
			ShardIteratorType: types.ShardIteratorTypeLatest,
			StreamName:        &streamName,
		})

		if err != nil {
			log.Printf("Failed to get shard iterator, %v\n", err)
			continue
		}

		shardIterators[*shard.ShardId] = shardIteratorResp.ShardIterator
	}

	kcc.shardIteratorMutex.Lock()
	kcc.shardIterators = shardIterators
	kcc.shardIteratorMutex.Unlock()
}

func (kcc *KinesisClient) IterateShards() {
	for {
		select {
		case <-(*kcc.ctx).Done():
			return
		default:
			{
				kcc.shardIteratorMutex.Lock()
				for shardId, shardIterator := range kcc.shardIterators {
					getRecordsInput := &kinesis.GetRecordsInput{
						ShardIterator: shardIterator,
						Limit:         &shardRecordLimit,
					}

					getRecordsResp, err := kcc.client.GetRecords(context.TODO(), getRecordsInput)
					if err != nil {
						log.Println(err)
						continue
					}

					shardIterator = getRecordsResp.NextShardIterator
					kcc.shardIterators[shardId] = shardIterator

					events := make([]beta9Types.Event, len(getRecordsResp.Records))
					for idx, record := range getRecordsResp.Records {
						event := beta9Types.Event{}
						err := json.Unmarshal(record.Data, &event)
						if err != nil {
							continue // silently skip event if it can't be unmarshalled
						}

						event.ApproximateArrivalTimestamp = record.ApproximateArrivalTimestamp
						events[idx] = event
					}

					if len(events) == 0 {
						continue
					}

					kcc.sinkMutex.Lock() // Prevent removal/addition of sinks while iterating
					for _, sink := range kcc.sinks {
						go (*sink)(events)
					}
					kcc.sinkMutex.Unlock()
				}
				kcc.shardIteratorMutex.Unlock()
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (kcc *KinesisClient) AddSink(sinkId string, sink *beta9Types.EventSink) error {
	kcc.sinkMutex.Lock()
	if _, exists := kcc.sinks[sinkId]; exists {
		kcc.sinkMutex.Unlock()
		return errors.New("sink already exists")
	}

	kcc.sinks[sinkId] = sink
	kcc.sinkMutex.Unlock()
	return nil
}

func (kcc *KinesisClient) RemoveSink(sinkId string) error {
	kcc.sinkMutex.Lock()
	if _, exists := kcc.sinks[sinkId]; !exists {
		kcc.sinkMutex.Unlock()
		return errors.New("sink does not exist")
	}

	delete(kcc.sinks, sinkId)
	kcc.sinkMutex.Unlock()
	return nil
}

func (kcc *KinesisClient) PushEvent(event beta9Types.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	_, err = kcc.client.PutRecord(context.TODO(), &kinesis.PutRecordInput{
		Data:         data,
		StreamName:   &kcc.streamName,
		PartitionKey: &event.Name,
	})
	if err != nil {
		return err
	}

	return nil
}
