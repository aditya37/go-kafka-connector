package kafkaconnector

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type (
	kafkaConsumer struct {
		consumer *kafka.Consumer
	}
)

func NewKafkaConsumer(config KafkaConnectorConfig) (IKafkaConsumer, error) {
	url := fmt.Sprintf("%s:%d", config.Host, config.Port)
	consume, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     url,
		"group.id":              config.GroupId,
		"broker.address.family": config.BrokerAddresFamily,
		"auto.offset.reset":     config.AutoOffsetReset,
	})
	log.Printf("Connected To Kafka Consumer: URL:%s With Group ID:%s",
		url,
		config.GroupId)
	if err != nil {
		return nil, err
	}
	return &kafkaConsumer{consumer: consume}, nil
}

func (c *kafkaConsumer) Subscribe(topic string, timeout time.Duration) (*kafka.Message, error) {
	if err := c.consumer.Subscribe(topic, nil); err != nil {
		return nil, err
	}
	if timeout > -1 {
		msg, err := c.consumer.ReadMessage(timeout)
		if err != nil {
			return nil, err
		}
		return msg, err
	}
	msg, err := c.consumer.ReadMessage(-1)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (c *kafkaConsumer) ValidateTopic(topic string) error {
	metaData, err := c.consumer.GetMetadata(&topic, false, 1000)
	if err != nil {
		return err
	}
	for _, val := range metaData.Topics {
		if val.Error.Code() == 3 {
			return errors.New("Unknown topic")
		}
	}
	return nil
}
