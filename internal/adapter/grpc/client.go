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
	// 1. АГРЕГАЦІЯ: Групуємо однакові переміщення
	// Ключ мапи: "WarehouseID|ProductID" -> Кількість
	type moveKey struct {
		WarehouseID string
		ProductID   string
	}

	// Зберігаємо об'єм та кількість для кожного маршруту
	aggregated := make(map[moveKey]struct {
		Count    int32
		VolumeM3 float64
	})

	for _, m := range plan.Moves {
		key := moveKey{
			WarehouseID: m.WarehouseID,
			ProductID:   m.ProductID,
		}

		current := aggregated[key]
		current.Count++
		current.VolumeM3 = m.VolumeM3 // Об'єм одиниці товару однаковий
		aggregated[key] = current
	}

	// 2. Map Domain -> Proto (вже згрупований)
	protoMoves := make([]*pb.Move, 0, len(aggregated))
	for key, data := range aggregated {
		protoMoves = append(protoMoves, &pb.Move{
			ProductId:   key.ProductID,
			WarehouseId: key.WarehouseID,
			VolumeM3:    data.VolumeM3,
			Quantity:    data.Count, // <-- Тут буде реальна сума (напр. 50)
		})
	}

	// ... (Unallocated items залишаються як були)
	protoUnallocated := make([]*pb.UnallocatedItem, 0, len(plan.UnallocatedItems))
	for _, u := range plan.UnallocatedItems {
		protoUnallocated = append(protoUnallocated, &pb.UnallocatedItem{
			ProductId: u.ID,
			VolumeM3:  u.VolumeM3,
			Reason:    "insufficient_capacity",
		})
	}

	req := &pb.DistributionPlan{
		RequestId:        "generated-id-placeholder", // Java його проігнорує або візьме з контексту
		Moves:            protoMoves,
		UnallocatedItems: protoUnallocated,
	}

	// 3. Network Call
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := c.remote.ProcessPlan(ctx, req)
	if err != nil {
		return fmt.Errorf("rpc call failed: %w", err)
	}

	return nil
}
