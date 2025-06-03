-- Create main laptops table
CREATE TABLE IF NOT EXISTS laptops (
    id INTEGER PRIMARY KEY,
    company VARCHAR(255) NOT NULL,
    product VARCHAR(255) NOT NULL,
    type_name VARCHAR(255) NOT NULL,
    price_euros NUMERIC(10, 2) NOT NULL,
    sale_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create summary tables for different laptop types
CREATE TABLE IF NOT EXISTS gaming_laptops (
    id INTEGER PRIMARY KEY,
    company VARCHAR(255) NOT NULL,
    product VARCHAR(255) NOT NULL,
    price_euros NUMERIC(10, 2) NOT NULL,
    sale_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS business_laptops (
    id INTEGER PRIMARY KEY,
    company VARCHAR(255) NOT NULL,
    product VARCHAR(255) NOT NULL,
    price_euros NUMERIC(10, 2) NOT NULL,
    sale_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create processing log table
CREATE TABLE IF NOT EXISTS file_processing_log (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    records_processed INTEGER NOT NULL,
    processing_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'SUCCESS'
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_laptops_type_name ON laptops(type_name);
CREATE INDEX IF NOT EXISTS idx_laptops_price ON laptops(price_euros);
CREATE INDEX IF NOT EXISTS idx_laptops_sale_date ON laptops(sale_date);