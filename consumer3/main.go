package main

import (
	"context"
	"fmt"
	kafka "github.com/segmentio/kafka-go"
)

func main() {
	topicName := "kafkAssignment"

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: "group2",
		Topic:   topicName,
	})

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Println("Consumer3 Message Fetched ------> Topic : ", msg.Topic, " Offset : ", msg.Offset, " Key : ", string(msg.Key), " Value : ", string(msg.Value))
	}

	if err := reader.Close(); err != nil {
		fmt.Println("Failed to Close the Writer & error is ", err.Error())
	}
}
