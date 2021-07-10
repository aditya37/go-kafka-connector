package kafkaconnector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type (
	kafkaProducer struct {
		producer *kafka.Producer
		admin    *kafka.AdminClient
	}
)

func NewKafkaProducer(config KafkaConnectorConfig) (IKafkaProducer, error) {
	// Declare instance
	url := fmt.Sprintf("%s:%d", config.Host, config.Port)

	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers": url,
	}
	prod, err := kafka.NewProducer(kafkaConfig)
	if err != nil {
		return nil, err
	}
	admin, err := kafka.NewAdminClient(kafkaConfig)
	if err != nil {
		return nil, err
	}
	return &kafkaProducer{producer: prod, admin: admin}, nil
}

// ConvertToJson....
func convertToJson(data interface{}) ([]byte, error) {
	output, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return output, nil
}

// Publish
func (p *kafkaProducer) Publish(topic string, datas interface{}) error {
	// Convert data to json format
	converted, err := convertToJson(datas)
	if err != nil {
		return err
	}
	// Channel for store kafka.event
	eventChan := make(chan kafka.Event)
	// Do Publish data
	if err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: converted,
	}, eventChan); err != nil {
		return err
	}
	// store event from channel
	events := <-eventChan
	msg := events.(*kafka.Message)
	if msg.TopicPartition.Error != nil {
		return errors.New(msg.TopicPartition.Error.Error())
	}
	close(eventChan)
	return nil
}

// CreateTopic...
func (p *kafkaProducer) CreateTopic(ctx context.Context, topic string, partition, replication int) (string, error) {
	result := ""

	results, err := p.admin.CreateTopics(ctx, []kafka.TopicSpecification{{
		Topic:             topic,
		NumPartitions:     partition,
		ReplicationFactor: replication,
	}})
	if err != nil {
		return result, err
	}
	for _, val := range results {
		if val.Error.Code() != 0 {
			return result, val.Error
		}
		result = val.Topic
	}
	// after create topic close connection
	p.admin.Close()
	return result, nil
}

// CloseProducer...
func (p *kafkaProducer) CloseProducer() {
	p.producer.Close()
}
