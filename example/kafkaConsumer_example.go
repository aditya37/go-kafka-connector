package example

import (
	"log"

	connector "github.com/aditya37/go-kafka-connector"
)

func KafkaConsumer() {
	conn, err := connector.NewKafkaConsumer(connector.KafkaConnectorConfig{
		Host:            "37.44.244.xxx",
		Port:            9092,
		GroupId:         "my-consumer",
		AutoOffsetReset: "latest",
	})
	if err != nil {
		log.Println(err)
	}
	for {
		msg, err := conn.Subscribe("go-topic", -1)
		if err != nil {
			log.Println(err)
		}
		log.Println(string(msg.Value))

	}
}
