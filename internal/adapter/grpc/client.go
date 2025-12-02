package grpc

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// Import the GENERATED code
	pb "github.com/antonchaban/warehouse-distribution-engine-go/gen/go/distribution"
	"github.com/antonchaban/warehouse-distribution-engine-go/internal/algorithm"
)

// Client adapts the gRPC interface to the domain ResultSender interface.
type Client struct {
	conn   *grpc.ClientConn
	remote pb.DistributionResultReceiverClient
}

// NewClient establishes a connection to the Java service.
func NewClient(javaServiceAddr string) (*Client, error) {
	// For internal microservices, we often use insecure credentials (no TLS)
	// unless strictly required by security policy.
	conn, err := grpc.NewClient(
		javaServiceAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to java service: %w", err)
	}

	client := pb.NewDistributionResultReceiverClient(conn)

	return &Client{
		conn:   conn,
		remote: client,
	}, nil
}

// Close cleans up the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// SendPlan converts the domain model to Proto and sends it over the wire.
// This satisfies the app.ResultSender interface.
func (c *Client) SendPlan(ctx context.Context, plan algorithm.DistributionPlan) error {
	// 1. Map Domain -> Proto
	protoMoves := make([]*pb.Move, 0, len(plan.Moves))
	for _, m := range plan.Moves {
		protoMoves = append(protoMoves, &pb.Move{
			ProductId:   m.ProductID,
			WarehouseId: m.WarehouseID,
			VolumeM3:    m.VolumeM3,
		})
	}

	protoUnallocated := make([]*pb.UnallocatedItem, 0, len(plan.UnallocatedItems))
	for _, u := range plan.UnallocatedItems {
		protoUnallocated = append(protoUnallocated, &pb.UnallocatedItem{
			ProductId: u.ID,
			VolumeM3:  u.VolumeM3,
			Reason:    "insufficient_capacity",
		})
	}

	req := &pb.DistributionPlan{
		// Note: request_id should ideally be passed through from the input event.
		// For now, we assume it's part of the context or passed separately,
		// but let's leave it empty or update the interface if needed.
		RequestId:        "generated-id",
		Moves:            protoMoves,
		UnallocatedItems: protoUnallocated,
	}

	// 2. Network Call with Timeout
	// Always set a short timeout for RPC calls to avoid cascading failures.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := c.remote.ProcessPlan(ctx, req)
	if err != nil {
		return fmt.Errorf("rpc call failed: %w", err)
	}

	return nil
}
