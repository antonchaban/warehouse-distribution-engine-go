package algorithm

import "errors"

// Product represents an item to be distributed.
type Product struct {
	ID       string
	VolumeM3 float64
	Priority int // Higher value (e.g., 10) means higher priority than (e.g., 1)
}

// Warehouse represents a storage facility state.
// Optimized for in-memory lookup O(1).
type Warehouse struct {
	ID              string
	TotalCapacityM3 float64
	CurrentStockM3  float64
	// IncomingM3 represents shipments currently in transit.
	// We must account for this to avoid overfilling.
	IncomingM3 float64
}

// AvailableCapacity calculates the real free space.
// Formula: Total - (Stock + Incoming)
func (w *Warehouse) AvailableCapacity() float64 {
	used := w.CurrentStockM3 + w.IncomingM3
	if used >= w.TotalCapacityM3 {
		return 0.0
	}
	return w.TotalCapacityM3 - used
}

// UtilizationRatio returns the fullness percentage (0.0 to 1.0).
// Used by the Priority Queue to find the least filled warehouse.
func (w *Warehouse) UtilizationRatio() float64 {
	if w.TotalCapacityM3 == 0 {
		return 1.0 // Treat as full to avoid division by zero
	}
	return (w.CurrentStockM3 + w.IncomingM3) / w.TotalCapacityM3
}

// CanFit checks if the product fits into the warehouse.
func (w *Warehouse) CanFit(item Product) bool {
	return w.AvailableCapacity() >= item.VolumeM3
}

// Allocate simulates placing a product into the warehouse.
// It updates the state in memory during the calculation loop.
func (w *Warehouse) Allocate(item Product) error {
	if !w.CanFit(item) {
		return errors.New("not enough capacity")
	}
	// We increase IncomingM3 because this item is now part of the plan
	w.IncomingM3 += item.VolumeM3
	return nil
}

// Move represents a single instruction in the distribution plan.
type Move struct {
	ProductID   string
	WarehouseID string
	VolumeM3    float64
}

// DistributionPlan is the final result of the algorithm.
type DistributionPlan struct {
	Moves            []Move
	UnallocatedItems []Product // Items that could not fit anywhere
}
