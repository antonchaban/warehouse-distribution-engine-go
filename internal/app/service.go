package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/antonchaban/warehouse-distribution-engine-go/internal/algorithm"
)

// StateProvider defines the contract for retrieving the current world state.
// This interface allows us to mock the data source (database) in tests.
type StateProvider interface {
	// FetchWorldState retrieves the current state of all warehouses.
	FetchWorldState(ctx context.Context) ([]*algorithm.Warehouse, error)

	// FetchPendingItems retrieves the products associated with a specific Supply ID.
	// Updated for Spec v2.
	FetchPendingItems(ctx context.Context, supplyID int64) ([]algorithm.Product, error)
}

// ResultSender defines the contract for sending the calculation result.
// Usually implemented by a gRPC client or a message queue producer.
type ResultSender interface {
	SendPlan(ctx context.Context, plan algorithm.DistributionPlan) error
}

// Service is the main orchestrator of the business logic.
// It connects the Infrastructure (Adapter) with the Core Domain (Algorithm).
type Service struct {
	provider StateProvider
	algo     algorithm.Packer
	sender   ResultSender
	logger   *slog.Logger
}

// NewService creates a new instance of the application service with required dependencies.
func NewService(
	p StateProvider,
	a algorithm.Packer,
	s ResultSender,
	l *slog.Logger,
) *Service {
	return &Service{
		provider: p,
		algo:     a,
		sender:   s,
		logger:   l,
	}
}

// CalculateDistribution executes the full distribution cycle.
// Flow: Load Data -> Run Algorithm -> Send Result.
func (s *Service) CalculateDistribution(ctx context.Context, requestID string, supplyID int64) error {
	s.logger.Info("starting distribution calculation",
		"request_id", requestID,
		"supply_id", supplyID,
	)

	// STEP 1: Load Data (IO Bound).
	// We fetch the current state from the database via the adapter.
	warehouses, err := s.provider.FetchWorldState(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch world state: %w", err)
	}

	// We fetch specific items for this supply batch
	items, err := s.provider.FetchPendingItems(ctx, supplyID)
	if err != nil {
		return fmt.Errorf("failed to fetch pending items: %w", err)
	}

	s.logger.Info("data loaded",
		"warehouses_count", len(warehouses),
		"items_count", len(items))

	// STEP 2: Execute Algorithm (CPU Bound).
	// This happens purely in memory using the Core Domain logic.
	plan := s.algo.Distribute(warehouses, items)

	s.logger.Info("calculation completed",
		"moves_generated", len(plan.Moves),
		"unallocated_count", len(plan.UnallocatedItems))

	// STEP 3: Send Result (IO Bound).
	// We send the generated plan back to the Java service via gRPC/MQ.
	if err := s.sender.SendPlan(ctx, plan); err != nil {
		return fmt.Errorf("failed to send distribution plan: %w", err)
	}

	s.logger.Info("distribution plan sent successfully", "request_id", requestID)
	return nil
}
