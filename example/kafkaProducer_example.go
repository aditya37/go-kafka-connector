package example

import (
	"log"
	"time"

	connector "github.com/aditya37/go-kafka-connector"
)

type DumyData struct {
	Nama string `json:"name,omitempty"`
}

func KafkaProducer() {
	kafkaConf := connector.KafkaConnectorConfig{
		Host: "37.44.244.xxx",
		Port: 9092,
	}
	conn, err := connector.NewKafkaProducer(kafkaConf)
	if err != nil {
		log.Println(err)
	}
	defer conn.CloseProducer()
	datas := &DumyData{
		Nama: "nama " + time.Now().String(),
	}

	if err := conn.Publish("go-topic", datas); err != nil {
		log.Println(err)
	}
}
