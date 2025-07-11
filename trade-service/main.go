package main

import (
	"context"
	"fmt"
	"log"
	"time"

	walletpb "github.com/Deivanai03/trade-engine-poc/proto/walletpb" // Import generated proto
	"google.golang.org/grpc"
)

func main() {
	fmt.Println("🚀 TradeService starting...")

	// Connect to WalletService gRPC server
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("❌ Could not connect: %v", err)
	}
	defer conn.Close()

	// Create a client for WalletService
	client := walletpb.NewWalletServiceClient(conn)

	// Call GetBalance for user123
	userID := "user123"
	fmt.Printf("📞 Requesting balances for user: %s\n", userID)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	req := &walletpb.BalanceRequest{UserId: userID}
	res, err := client.GetBalance(ctx, req)
	if err != nil {
		log.Fatalf("❌ Error calling GetBalance: %v", err)
	}

	fmt.Println("✅ Received balances:")
	for currency, amount := range res.GetBalances() {
		fmt.Printf("   %s: %.4f\n", currency, amount)
	}
}
