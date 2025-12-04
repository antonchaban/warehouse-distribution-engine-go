package postgres

import (
	"context"
	"fmt"

	"github.com/antonchaban/warehouse-distribution-engine-go/internal/algorithm"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Repository implements the data access layer using PostgreSQL.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a new instance of the postgres adapter.
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// FetchWorldState loads the entire network state into domain models.
// Updated for Spec v2: includes 'PLANNED' shipments in capacity calculation.
func (r *Repository) FetchWorldState(ctx context.Context) ([]*algorithm.Warehouse, error) {
	const query = `
	WITH stock_agg AS (
		SELECT 
			sl.warehouse_id,
			COALESCE(SUM(sl.quantity * p.volume_m3), 0) as occupied_volume
		FROM stock_levels sl
		JOIN products p ON sl.product_id = p.id
		GROUP BY sl.warehouse_id
	),
	incoming_agg AS (
		SELECT 
			s.destination_id as warehouse_id,
			COALESCE(SUM(si.quantity * p.volume_m3), 0) as incoming_volume
		FROM shipments s
		JOIN shipment_items si ON s.id = si.shipment_id
		JOIN products p ON si.product_id = p.id
		-- Critical update: Include PLANNED shipments in capacity check (Spec v2)
		WHERE s.status IN ('IN_TRANSIT', 'PLANNED')
		GROUP BY s.destination_id
	)
	SELECT 
		w.id,
		w.total_capacity,
		COALESCE(sa.occupied_volume, 0) as current_stock,
		COALESCE(ia.incoming_volume, 0) as incoming_stock
	FROM warehouses w
	LEFT JOIN stock_agg sa ON w.id = sa.warehouse_id
	LEFT JOIN incoming_agg ia ON w.id = ia.warehouse_id;
	`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query world state: %w", err)
	}
	defer rows.Close()

	var warehouses []*algorithm.Warehouse

	for rows.Next() {
		var w algorithm.Warehouse
		err := rows.Scan(
			&w.ID,
			&w.TotalCapacityM3,
			&w.CurrentStockM3,
			&w.IncomingM3,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan warehouse row: %w", err)
		}
		warehouses = append(warehouses, &w)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating warehouse rows: %w", err)
	}

	return warehouses, nil
}

// FetchPendingItems retrieves items that need to be distributed.
// FIXED: Now accepts supplyID (int64) to match the interface signature.
func (r *Repository) FetchPendingItems(ctx context.Context, supplyID int64) ([]algorithm.Product, error) {
	// Updated query: Read from supply_items based on the supply_id received from RabbitMQ
	const query = `
		SELECT 
			p.id,
			p.volume_m3,
			si.quantity,
			10 as priority -- Default priority
		FROM supply_items si
		JOIN products p ON si.product_id = p.id
		WHERE si.supply_id = $1
	`

	rows, err := r.pool.Query(ctx, query, supplyID)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending items: %w", err)
	}
	defer rows.Close()

	var items []algorithm.Product

	for rows.Next() {
		var (
			productID string
			volume    float64
			quantity  int
			priority  int
		)

		if err := rows.Scan(&productID, &volume, &quantity, &priority); err != nil {
			return nil, fmt.Errorf("failed to scan supply item: %w", err)
		}

		// Expand quantity into individual items for the Bin Packing algorithm
		for i := 0; i < quantity; i++ {
			items = append(items, algorithm.Product{
				ID:       productID,
				VolumeM3: volume,
				Priority: priority,
			})
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pending item rows: %w", err)
	}

	return items, nil
}
