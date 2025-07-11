package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/Deivanai03/trade-engine-poc/proto/walletpb" // Import generated proto
	"google.golang.org/grpc"
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
	fmt.Println("🚀 WalletService starting on port 50051...")

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

	// Start listening on port 50051
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Serve incoming connections
	if err := server.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
