package main

import (
	"context"
	"fmt"
	kafka "github.com/segmentio/kafka-go"
)

func main() {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  "kafkaAssignment",
		RequiredAcks:           kafka.RequireAll,
		Async:                  true,
		AllowAutoTopicCreation: true,
		Completion: func(msgs []kafka.Message, err error) {
			if err != nil {
				fmt.Println("Error occurred ", err.Error())
				return
			}

			for _, val := range msgs {
				fmt.Println("Message sent Successfully ------> Topic : ", val.Topic, " Offset : ", val.Offset, " Key : ", string(val.Key), " Value : ", string(val.Value))
			}
		},
	}
	err := writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Greet"),
			Value: []byte("Hello Everyone!"),
		},
		kafka.Message{
			Key:   []byte("Name"),
			Value: []byte("I am Devata Dinesh"),
		},
		kafka.Message{
			Key:   []byte("Lesson"),
			Value: []byte("Kafka with Golang"),
		},
		kafka.Message{
			Key:   []byte("Bye"),
			Value: []byte("Bye!!!"),
		},
	)
	if err != nil {
		fmt.Println("Producer failed to write the messages to the topic : ", writer.Topic, "error : ", err.Error())
	}

	if err := writer.Close(); err != nil {
		fmt.Println("Failed to Close the Writer & error is ", err.Error())
	}
}
