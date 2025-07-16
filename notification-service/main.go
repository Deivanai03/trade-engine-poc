package main

import (
	"context"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

// Kafka config
var (
	brokers       = []string{"localhost:9092"} // Kafka brokers
	topic         = "order-events"             // Topic to subscribe
	consumerGroup = "notification-group"       // Unique consumer group
)

func main() {
	fmt.Println("🚀 NotificationService starting...")

	// Set up consumer group
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Return.Errors = true

	client, err := sarama.NewConsumerGroup(brokers, consumerGroup, config)
	if err != nil {
		log.Fatalf("❌ Error creating consumer group client: %v", err)
	}
	defer client.Close()

	handler := ConsumerGroupHandler{}

	for {
		err := client.Consume(context.Background(), []string{topic}, handler)
		if err != nil {
			log.Fatalf("❌ Error consuming from topic: %v", err)
		}
	}
}

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler
type ConsumerGroupHandler struct{}

func (ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	fmt.Println("🔔 NotificationService joined consumer group.")
	return nil
}

func (ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (ConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("📧 [Notification] Order notification: %s\n", string(msg.Value))
		// Mark message as processed
		sess.MarkMessage(msg, "")
	}
	return nil
}
