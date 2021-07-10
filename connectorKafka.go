package kafkaconnector

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type (
	// Producer...
	IKafkaProducer interface {
		// Publish or produce some event to created topic
		Publish(topic string, datas interface{}) error
		// Create some topic
		CreateTopic(ctx context.Context, topic string, partition, replication int) (string, error)
		// Close connection after create or publish
		CloseProducer()
	}
	// Consumer...
	IKafkaConsumer interface {
		// Subcsribe some topic
		Subscribe(topic string, timeout time.Duration) (*kafka.Message, error)
		// Validate topic
		ValidateTopic(topic string) error
	}
	// Global config for connect to kafka broker
	// Example:
	// Host : 37.44.244.xxx
	// Port : 9092
	// GroupId : your-consumer-group
	// Offset: latest or erlier
	// BrokerAddresFamily : v4/v6
	KafkaConnectorConfig struct {
		Host               string
		Port               int
		BrokerAddresFamily string
		GroupId            string
		AutoOffsetReset    string
	}
)
