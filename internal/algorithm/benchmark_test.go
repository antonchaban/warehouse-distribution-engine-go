package algorithm

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

// --- 1. Baseline Algorithm (First-Fit) ---
func DistributeFirstFit(warehouses []*Warehouse, items []Product) DistributionPlan {
	plan := DistributionPlan{Moves: []Move{}}

	whCopies := make([]*Warehouse, len(warehouses))
	for i, w := range warehouses {
		whCopies[i] = &Warehouse{
			ID:              w.ID,
			TotalCapacityM3: w.TotalCapacityM3,
			CurrentStockM3:  w.CurrentStockM3,
			IncomingM3:      w.IncomingM3,
		}
	}

	for _, item := range items {
		allocated := false
		for _, wh := range whCopies {
			if wh.CanFit(item) {
				_ = wh.Allocate(item)
				plan.Moves = append(plan.Moves, Move{WarehouseID: wh.ID, ProductID: item.ID, VolumeM3: item.VolumeM3})
				allocated = true
				break
			}
		}
		if !allocated {
			plan.UnallocatedItems = append(plan.UnallocatedItems, item)
		}
	}
	return plan
}

// --- 2. Helper: Генерація даних (ВИПРАВЛЕНО) ---
func generateScenario() ([]*Warehouse, []Product) {
	// 10 великих складів однакової ємності (щоб краще бачити дисбаланс)
	warehouses := make([]*Warehouse, 10)
	for i := 0; i < 10; i++ {
		warehouses[i] = &Warehouse{
			ID:              fmt.Sprintf("WH-%d", i),
			TotalCapacityM3: 1000.0, // Усі по 1000, всього 10 000
		}
	}

	// 2500 товарів по ~2.0 м3 = ~5000 м3 загалом.
	// Це 50% завантаження системи.
	items := make([]Product, 2500)
	for i := 0; i < 2500; i++ {
		items[i] = Product{
			ID:       fmt.Sprintf("ITEM-%d", i),
			VolumeM3: 1.0 + rand.Float64()*2.0, // 1.0 - 3.0 м3
		}
	}
	return warehouses, items
}

// --- 3. Helper: Розрахунок Коефіцієнта Варіації (CV) ---
func calculateCV(warehouses []*Warehouse, plan DistributionPlan) float64 {
	// 1. Рахуємо реальне завантаження
	usageMap := make(map[string]float64)
	for _, m := range plan.Moves {
		usageMap[m.WarehouseID] += m.VolumeM3
	}

	// 2. Рахуємо відсоток заповненості для кожного складу
	var loadRatios []float64
	var sumRatios float64

	for _, w := range warehouses {
		used := usageMap[w.ID] // Тільки те, що додали (для чистоти експерименту)
		ratio := used / w.TotalCapacityM3
		loadRatios = append(loadRatios, ratio)
		sumRatios += ratio
	}

	// 3. Статистика
	mean := sumRatios / float64(len(warehouses))

	if mean == 0 {
		return 0
	}

	var varianceSum float64
	for _, r := range loadRatios {
		varianceSum += math.Pow(r-mean, 2)
	}

	stdDev := math.Sqrt(varianceSum / float64(len(warehouses)))

	return stdDev / mean // Coefficient of Variation
}

// --- 4. ЕКСПЕРИМЕНТ ---
func TestCompareAlgorithms(t *testing.T) {
	warehouses, items := generateScenario()

	// A. First-Fit
	baselinePlan := DistributeFirstFit(warehouses, items)
	baselineCV := calculateCV(warehouses, baselinePlan)

	// B. WFD (Ваш алгоритм)
	// Копіюємо для чистоти
	whForWFD := make([]*Warehouse, len(warehouses))
	for i, w := range warehouses {
		whForWFD[i] = &Warehouse{ID: w.ID, TotalCapacityM3: w.TotalCapacityM3}
	}

	packer := NewWFDAlgorithm()
	wfdPlan := packer.Distribute(whForWFD, items)
	wfdCV := calculateCV(whForWFD, wfdPlan)

	fmt.Println("\n=== РЕЗУЛЬТАТИ ЕКСПЕРИМЕНТУ (Load Balancing) ===")
	fmt.Printf("Вхідні дані: %d складів, %d товарів (Load ~50%%)\n", len(warehouses), len(items))
	fmt.Println("-------------------------------------------------------")
	fmt.Printf("| %-15s | %-12s | %-12s |\n", "Алгоритм", "CV (Дисбаланс)", "Unallocated")
	fmt.Println("-------------------------------------------------------")
	fmt.Printf("| %-15s | %-12.4f | %-12d |\n", "First-Fit", baselineCV, len(baselinePlan.UnallocatedItems))
	fmt.Printf("| %-15s | %-12.4f | %-12d |\n", "WFD (Proposed)", wfdCV, len(wfdPlan.UnallocatedItems))
	fmt.Println("-------------------------------------------------------")

	if wfdCV < baselineCV {
		improvement := (baselineCV - wfdCV) / baselineCV * 100
		t.Logf("Успіх! WFD розподілив навантаження на %.2f%% рівномірніше.", improvement)
	} else {
		t.Log("WFD не показав кращого балансування.")
	}
}
