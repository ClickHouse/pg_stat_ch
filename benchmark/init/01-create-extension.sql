-- Benchmark database initialization for pg_stat_ch
-- Creates extension and benchmark schema with sample data

-- Create pg_stat_ch extension
CREATE EXTENSION IF NOT EXISTS pg_stat_ch;

-- Create benchmark schema
CREATE SCHEMA IF NOT EXISTS bench;

-- Users table
CREATE TABLE bench.users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL,
    balance DECIMAL(15, 2) DEFAULT 0.00,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_users_status ON bench.users(status);
CREATE INDEX idx_users_created_at ON bench.users(created_at);

-- Products/inventory table
CREATE TABLE bench.inventory (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    quantity INTEGER DEFAULT 0,
    category VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_inventory_category ON bench.inventory(category);
CREATE INDEX idx_inventory_price ON bench.inventory(price);

-- Orders table
CREATE TABLE bench.orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES bench.users(id),
    product_id INTEGER REFERENCES bench.inventory(id),
    quantity INTEGER NOT NULL,
    total_price DECIMAL(15, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_orders_user_id ON bench.orders(user_id);
CREATE INDEX idx_orders_status ON bench.orders(status);
CREATE INDEX idx_orders_created_at ON bench.orders(created_at);

-- Transactions table
CREATE TABLE bench.transactions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES bench.users(id),
    order_id INTEGER REFERENCES bench.orders(id),
    amount DECIMAL(15, 2) NOT NULL,
    type VARCHAR(20) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_transactions_user_id ON bench.transactions(user_id);
CREATE INDEX idx_transactions_type ON bench.transactions(type);
CREATE INDEX idx_transactions_created_at ON bench.transactions(created_at);

-- Seed users (10,000 records)
INSERT INTO bench.users (username, email, balance, status)
SELECT
    'user_' || i,
    'user_' || i || '@example.com',
    (random() * 10000)::DECIMAL(15, 2),
    CASE (random() * 3)::INTEGER
        WHEN 0 THEN 'active'
        WHEN 1 THEN 'inactive'
        ELSE 'pending'
    END
FROM generate_series(1, 10000) AS i;

-- Seed inventory (1,000 products)
INSERT INTO bench.inventory (sku, name, price, quantity, category)
SELECT
    'SKU-' || LPAD(i::TEXT, 6, '0'),
    'Product ' || i,
    (random() * 1000 + 1)::DECIMAL(10, 2),
    (random() * 1000)::INTEGER,
    CASE (random() * 5)::INTEGER
        WHEN 0 THEN 'electronics'
        WHEN 1 THEN 'clothing'
        WHEN 2 THEN 'books'
        WHEN 3 THEN 'home'
        ELSE 'sports'
    END
FROM generate_series(1, 1000) AS i;

-- Seed some initial orders (5,000 records)
INSERT INTO bench.orders (user_id, product_id, quantity, total_price, status)
SELECT
    (random() * 9999 + 1)::INTEGER,
    (random() * 999 + 1)::INTEGER,
    (random() * 5 + 1)::INTEGER,
    (random() * 500 + 10)::DECIMAL(15, 2),
    CASE (random() * 4)::INTEGER
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'processing'
        WHEN 2 THEN 'shipped'
        ELSE 'completed'
    END
FROM generate_series(1, 5000) AS i;

-- Seed some initial transactions (10,000 records)
INSERT INTO bench.transactions (user_id, order_id, amount, type, description)
SELECT
    (random() * 9999 + 1)::INTEGER,
    CASE WHEN random() > 0.3 THEN (random() * 4999 + 1)::INTEGER ELSE NULL END,
    (random() * 500 + 1)::DECIMAL(15, 2),
    CASE (random() * 3)::INTEGER
        WHEN 0 THEN 'credit'
        WHEN 1 THEN 'debit'
        ELSE 'refund'
    END,
    'Transaction ' || i
FROM generate_series(1, 10000) AS i;

-- Update statistics
ANALYZE bench.users;
ANALYZE bench.inventory;
ANALYZE bench.orders;
ANALYZE bench.transactions;

-- Verify pg_stat_ch is loaded
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_ch') THEN
        RAISE NOTICE 'pg_stat_ch extension loaded successfully';
    ELSE
        RAISE WARNING 'pg_stat_ch extension NOT loaded';
    END IF;
END $$;
