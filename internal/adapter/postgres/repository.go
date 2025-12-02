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
// It executes a complex aggregated query to minimize network traffic.
func (r *Repository) FetchWorldState(ctx context.Context) ([]*algorithm.Warehouse, error) {
	// SQL Query Design:
	// We use CTEs to aggregate stock and incoming shipments separately to avoid Cartesian products.
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
		WHERE s.status = 'IN_TRANSIT'
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
// This implements the missing method of the StateProvider interface.
func (r *Repository) FetchPendingItems(ctx context.Context) ([]algorithm.Product, error) {
	// We assume there are shipments with status 'PENDING' containing items to distribute.
	// We join with the products table to get dimensions.
	const query = `
		SELECT 
			p.id,
			p.volume_m3,
			si.quantity,
			10 as priority -- Default priority (could be fetched from products or orders)
		FROM shipment_items si
		JOIN products p ON si.product_id = p.id
		JOIN shipments s ON si.shipment_id = s.id
		WHERE s.status = 'PENDING'
	`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending items: %w", err)
	}
	defer rows.Close()

	var items []algorithm.Product

	// Iterate over rows. Note: A row represents a "Batch" (e.g., "iPhone x 50").
	// The algorithm works with individual items, so we must expand the quantity.
	for rows.Next() {
		var (
			productID string
			volume    float64
			quantity  int
			priority  int
		)

		if err := rows.Scan(&productID, &volume, &quantity, &priority); err != nil {
			return nil, fmt.Errorf("failed to scan pending item: %w", err)
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
