package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
    topic := "topic-example"

    p, err := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers": "localhost:9092",
    })
    if err != nil {
        fmt.Printf("Failed to create producer: %s\n", err)
        os.Exit(1)
    }

    go func() {
        for e := range p.Events() {
            switch ev := e.(type) {
            case *kafka.Message:
                if ev.TopicPartition.Error != nil {
                    fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
                } else {
                    fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n", *ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
                }
            }
        }
    } ()

    users := map[string]string{
        "1": "Adi",
        "2": "Yuma",
        "3": "Salsa",
        "4": "Edi",
    }

    for key, val := range users {
        p.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
            Key: []byte(key),
            Value: []byte(val),
        }, nil)
    }

    p.Flush(15 * 1000)
    p.Close()
}