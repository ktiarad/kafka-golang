package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
    topic := "topic-example"
    
    c, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": "localhost:9092",
        "group.id": "test-consumer-group",
    })
    if err != nil {
        fmt.Printf("Failed to consume: %s\n", err)
        os.Exit(1)
    }

    err = c.SubscribeTopics([]string{topic}, nil)

    defer func() {
        fmt.Printf("Error when subscribe: %s\n", err)
    } ()

    sigchan := make(chan os.Signal, 1)
    signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

    run := true
    for run {
        select {
        case sig := <- sigchan:
            fmt.Printf("Caught signal %v: terminating\n", sig)
            run = false
        default:
            ev, err := c.ReadMessage(100 * time.Millisecond)
            if err != nil {
                continue
            }

            fmt.Printf("Consumed event from topic %s: key = %-10s value = %s\n", *ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
        }
    }

    c.Close()
}