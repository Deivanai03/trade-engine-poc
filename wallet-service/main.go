package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"proto/walletpb" // Import generated proto

	"github.com/IBM/sarama"
	"google.golang.org/grpc"
)

// Kafka config
var (
	brokers       = []string{"localhost:9092"} // Kafka brokers
	topic         = "order-events"             // Topic to subscribe
	consumerGroup = "wallet-group"             // Unique consumer group for wallet service
)

// walletServer implements WalletServiceServer interface
type walletServer struct {
	walletpb.UnimplementedWalletServiceServer                               // To satisfy interface
	balances                                  map[string]map[string]float64 // Dummy DB: user_id -> currency -> amount
}

// GetBalance returns user's crypto balances
func (s *walletServer) GetBalance(ctx context.Context, req *walletpb.BalanceRequest) (*walletpb.BalanceResponse, error) {
	userID := req.GetUserId()
	fmt.Printf("Received GetBalance request for user: %s\n", userID)

	userBalances, exists := s.balances[userID]
	if !exists {
		return &walletpb.BalanceResponse{Balances: map[string]float64{}}, nil
	}

	return &walletpb.BalanceResponse{Balances: userBalances}, nil
}

func main() {
	fmt.Println("🚀 WalletService starting...")

	// Start gRPC server
	go startGRPCServer()

	// Start Kafka consumer group
	startKafkaConsumerGroup()
}

func startGRPCServer() {

	// Dummy balances
	balances := map[string]map[string]float64{
		"user123": {
			"BTC":  1.23,
			"ETH":  4.56,
			"USDT": 1000.00,
		},
		"user456": {
			"BTC": 0.75,
			"ETH": 2.1,
		},
	}

	// Create gRPC server
	server := grpc.NewServer()
	walletpb.RegisterWalletServiceServer(server, &walletServer{balances: balances})

	port := os.Getenv("WALLET_GRPC_PORT")
	if port == "" {
		port = "50051" // default
	}

	// Start listening on port
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Serve incoming connections
	if err := server.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// --- New Consumer Group Handler for Wallet Service ---
type walletConsumerGroupHandler struct{}

func (walletConsumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	fmt.Println("🔑 WalletService joined consumer group. Assigned partitions:")
	for topic, partitions := range sess.Claims() {
		fmt.Printf("   Topic: %s, Partitions: %v\n", topic, partitions)
	}
	return nil
}

func (walletConsumerGroupHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	fmt.Println("🔑 WalletService leaving consumer group.")
	return nil
}

func (walletConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Printf("👀 WalletService consuming partition %d\n", claim.Partition())
	for msg := range claim.Messages() {
		fmt.Printf("✅ [Kafka] WalletService received message: %s (partition=%d offset=%d)\n", string(msg.Value), msg.Partition, msg.Offset)
		// TODO: Deduct balance for user
		sess.MarkMessage(msg, "")
	}
	return nil
}

func startKafkaConsumerGroup() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Return.Errors = true

	client, err := sarama.NewConsumerGroup(brokers, consumerGroup, config)
	if err != nil {
		log.Fatalf("❌ Error creating consumer group client: %v", err)
	}
	defer client.Close()

	handler := walletConsumerGroupHandler{}
	ctx := context.Background()

	for {
		fmt.Println("🔄 WalletService waiting for messages...")
		err := client.Consume(ctx, []string{topic}, handler)
		if err != nil {
			log.Fatalf("❌ Error consuming from topic: %v", err)
		}
	}
}
