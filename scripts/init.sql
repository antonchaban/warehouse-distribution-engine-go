-- scripts/init.sql

CREATE TABLE IF NOT EXISTS warehouses (
                                          id VARCHAR(50) PRIMARY KEY,
    total_capacity NUMERIC(10, 2) NOT NULL
    );

CREATE TABLE IF NOT EXISTS products (
                                        id VARCHAR(50) PRIMARY KEY,
    volume_m3 NUMERIC(10, 3) NOT NULL
    );

CREATE TABLE IF NOT EXISTS stock_levels (
                                            warehouse_id VARCHAR(50) REFERENCES warehouses(id),
    product_id VARCHAR(50) REFERENCES products(id),
    quantity INT NOT NULL DEFAULT 0,
    PRIMARY KEY (warehouse_id, product_id)
    );

CREATE TABLE IF NOT EXISTS shipments (
                                         id VARCHAR(50) PRIMARY KEY,
    status VARCHAR(20) NOT NULL, -- 'PENDING', 'IN_TRANSIT', 'DELIVERED'
    destination_id VARCHAR(50) REFERENCES warehouses(id)
    );

CREATE TABLE IF NOT EXISTS shipment_items (
                                              shipment_id VARCHAR(50) REFERENCES shipments(id),
    product_id VARCHAR(50) REFERENCES products(id),
    quantity INT NOT NULL
    );

-- --- SEED DATA (Тестові дані) ---

-- 1. Склади (Warehouses)
-- WH-BIG: Майже повний (Load Balancing має уникати його)
INSERT INTO warehouses (id, total_capacity) VALUES ('WH-BIG', 100.0);
-- WH-SMALL: Порожній (Алгоритм має обрати його)
INSERT INTO warehouses (id, total_capacity) VALUES ('WH-SMALL', 50.0);

-- 2. Товари (Products)
INSERT INTO products (id, volume_m3) VALUES ('IPHONE', 0.1);
INSERT INTO products (id, volume_m3) VALUES ('FRIDGE', 2.0);

-- 3. Поточні залишки (Stock)
-- WH-BIG заповнений на 90 одиниць (90% capacity used)
INSERT INTO stock_levels (warehouse_id, product_id, quantity) VALUES ('WH-BIG', 'FRIDGE', 45); -- 45 * 2.0 = 90.0 m3

-- 4. Запити на розподіл (Pending Shipments)
-- Ми створюємо "віртуальну" накладну, яку треба розподілити
INSERT INTO shipments (id, status, destination_id) VALUES ('SHIP-001', 'PENDING', NULL);
INSERT INTO shipment_items (shipment_id, product_id, quantity) VALUES ('SHIP-001', 'FRIDGE', 2); -- 4 m3 total