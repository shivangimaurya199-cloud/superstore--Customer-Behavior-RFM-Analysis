use thansimoldb;

CREATE TABLE dim_customer (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    region VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    city VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_region (region),
    INDEX idx_state (state),
    INDEX idx_city (city)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE dim_date (
    date_id INT UNSIGNED PRIMARY KEY,
    date_value DATE NOT NULL UNIQUE,
    year SMALLINT NOT NULL,
    quarter TINYINT NOT NULL,
    month TINYINT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    day TINYINT NOT NULL,
    day_of_week TINYINT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    week_of_year TINYINT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    INDEX idx_year_month (year, month),
    INDEX idx_quarter (quarter)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE dim_product (
    product_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(150) NOT NULL,
    category VARCHAR(50) NOT NULL,
    sub_category VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_product (product_name, category, sub_category),
    INDEX idx_category (category),
    INDEX idx_sub_category (sub_category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE fact_sales (
    sales_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(20) NOT NULL,
    order_date_id INT UNSIGNED NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    product_id INT UNSIGNED NOT NULL,
    quantity INT NOT NULL DEFAULT 0,
    sales_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    discount DECIMAL(5,4) NOT NULL DEFAULT 0.0000,
    profit DECIMAL(12,2) NOT NULL DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fact_sales (
    sales_id INT AUTO_INCREMENT PRIMARY KEY,
    order_date_id INT NOT NULL,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    quantity INT,
    sales DECIMAL(10,2),
    profit DECIMAL(10,2),

    CONSTRAINT fk_date
        FOREIGN KEY (order_date_id)
        REFERENCES dim_date(date_id),

    CONSTRAINT fk_product
        FOREIGN KEY (product_id)
        REFERENCES dim_product(product_id),

    CONSTRAINT fk_customer
        FOREIGN KEY (customer_id)
        REFERENCES dim_customer(customer_id)
)
ENGINE = InnoDB;

-- Clean and populate dim_customer
INSERT INTO dim_customer (customer_id, customer_name, region, state, city)
SELECT DISTINCT
    TRIM(customer_id) AS customer_id,
    TRIM(COALESCE(customer_name, 'Unknown')) AS customer_name,
    TRIM(COALESCE(region, 'Unknown')) AS region,
    TRIM(COALESCE(state, 'Unknown')) AS state,
    TRIM(COALESCE(city, 'Unknown')) AS city
FROM thansimoldb
WHERE customer_id IS NOT NULL 
  AND TRIM(customer_id) != ''
ON DUPLICATE KEY UPDATE
    customer_name = VALUES(customer_name),
    region = VALUES(region),
    state = VALUES(state),
    city = VALUES(city);
    

INSERT INTO dim_product (product_name, category, sub_category)
SELECT DISTINCT
    TRIM(COALESCE(Product_Name, 'Unknown')) AS product_name,
    TRIM(COALESCE(Product_Category, 'Unknown')) AS category,
    TRIM(COALESCE(Product_Sub_Category, 'Unknown')) AS sub_category
FROM superstoredata
WHERE Product_Name IS NOT NULL 
  AND TRIM(Product_Name) != ''
ON DUPLICATE KEY UPDATE
    category = VALUES(category),
    sub_category = VALUES(sub_category);
    
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,
    date_value DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    day INT,
    day_of_week INT,
    day_name VARCHAR(20),
    week_of_year INT,
    is_weekend TINYINT(1)
);

DELIMITER $$

CREATE PROCEDURE populate_dim_date (
    IN p_start_date DATE,
    IN p_end_date DATE
)
BEGIN
    DECLARE v_date DATE;
    SET v_date = p_start_date;

    WHILE v_date <= p_end_date DO

        INSERT INTO dim_date (
            date_id,
            date_value,
            year,
            quarter,
            month,
            month_name,
            day,
            day_of_week,
            day_name,
            week_of_year,
            is_weekend
        )
        VALUES (
            DATE_FORMAT(v_date, '%Y%m%d'),
            v_date,
            YEAR(v_date),
            QUARTER(v_date),
            MONTH(v_date),
            MONTHNAME(v_date),
            DAY(v_date),
            DAYOFWEEK(v_date),
            DAYNAME(v_date),
            WEEKOFYEAR(v_date),
            IF(DAYOFWEEK(v_date) IN (1,7), 1, 0)
        );

        SET v_date = DATE_ADD(v_date, INTERVAL 1 DAY);

    END WHILE;
END$$

DELIMITER ;

CALL populate_dim_date('2022-01-01', '2022-12-31');

SELECT COUNT(*) FROM dim_date;
SELECT * FROM dim_date LIMIT 5;



INSERT INTO fact_sales (
    order_id,
    order_date_id,
    customer_id,
    product_id,
    quantity,
    sales_amount,
    discount,
    profit
)
SELECT
    TRIM(s.order_id) AS order_id,
    CAST(DATE_FORMAT(
        STR_TO_DATE(s.order_date, '%m/%d/%Y'), 
        '%Y%m%d'
    ) AS UNSIGNED) AS order_date_id,
    TRIM(s.customer_id) AS customer_id,
    p.product_id,
    COALESCE(s.Quantity_Ordered_new, 0) AS quantity,
    COALESCE(s.sales, 0) AS sales_amount,
    COALESCE(s.discount, 0) AS discount,
    COALESCE(s.profit, 0) AS profit
FROM superstoredata AS s
INNER JOIN dim_product AS p
    ON TRIM(s.Product_Name) = p.product_name
   AND TRIM(COALESCE(s.Product_Category, 'Unknown')) = p.category
   AND TRIM(COALESCE(s.Product_Sub_Category, 'Unknown')) = p.sub_category
INNER JOIN dim_customer AS c
    ON TRIM(s.customer_id) = c.customer_id
WHERE s.order_id IS NOT NULL
  AND s.customer_id IS NOT NULL
  AND s.order_date IS NOT NULL
  AND s.sales > 0
  AND STR_TO_DATE(s.order_date, '%m/%d/%Y') IS NOT NULL;
  
 
SELECT 'Orphaned Customer Records' AS check_name, COUNT(*) AS count
FROM fact_sales fs
LEFT JOIN dim_customer dc ON fs.customer_id = dc.customer_id
WHERE dc.customer_id IS NULL

UNION ALL

SELECT 'Orphaned Product Records', COUNT(*)
FROM fact_sales fs
LEFT JOIN dim_product dp ON fs.product_id = dp.product_id
WHERE dp.product_id IS NULL

UNION ALL

SELECT 'Orphaned Date Records', COUNT(*)
FROM fact_sales fs
LEFT JOIN dim_date dd ON fs.order_date_id = dd.date_id
WHERE dd.date_id IS NULL

UNION ALL

SELECT 'Negative Sales', COUNT(*)
FROM fact_sales
WHERE sales_amount < 0

UNION ALL

SELECT 'Negative Quantity', COUNT(*)
FROM fact_sales
WHERE quantity < 0;
 

SELECT 
    dd.year,
    dd.month_name,
    dc.region,
    COUNT(DISTINCT fs.order_id) AS total_orders,
    SUM(fs.quantity) AS total_quantity,
    SUM(fs.sales_amount) AS total_sales,
    SUM(fs.profit) AS total_profit,
    AVG(fs.sales_amount) AS avg_order_value
FROM fact_sales fs
INNER JOIN dim_date dd ON fs.order_date_id = dd.date_id
INNER JOIN dim_customer dc ON fs.customer_id = dc.customer_id
GROUP BY dd.year, dd.month, dd.month_name, dc.region
ORDER BY dd.year DESC, dd.month DESC, total_sales DESC;

SELECT 
    dp.category,
    dp.sub_category,
    dp.product_name,
    COUNT(DISTINCT fs.order_id) AS order_count,
    SUM(fs.quantity) AS units_sold,
    SUM(fs.sales_amount) AS revenue,
    SUM(fs.profit) AS profit,
    ROUND(SUM(fs.profit) / SUM(fs.sales_amount) * 100, 2) AS profit_margin_pct
FROM fact_sales fs
INNER JOIN dim_product dp ON fs.product_id = dp.product_id
GROUP BY dp.category, dp.sub_category, dp.product_name
HAVING revenue > 1000
ORDER BY revenue DESC
LIMIT 50;

SELECT 
    dc.customer_id,
    dc.customer_name,
    dc.region,
    dc.state,
    COUNT(DISTINCT fs.order_id) AS total_orders,
    SUM(fs.sales_amount) AS lifetime_value,
    AVG(fs.sales_amount) AS avg_order_value,
    MAX(dd.date_value) AS last_order_date,
    CASE 
        WHEN SUM(fs.sales_amount) > 10000 THEN 'VIP'
        WHEN SUM(fs.sales_amount) > 5000 THEN 'High Value'
        WHEN SUM(fs.sales_amount) > 1000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment
FROM fact_sales fs
INNER JOIN dim_customer dc ON fs.customer_id = dc.customer_id
INNER JOIN dim_date dd ON fs.order_date_id = dd.date_id
GROUP BY dc.customer_id, dc.customer_name, dc.region, dc.state
ORDER BY lifetime_value DESC;

SELECT 
    dd.year,
    dd.quarter,
    dp.category,
    SUM(fs.sales_amount) AS sales,
    SUM(fs.profit) AS profit,
    COUNT(DISTINCT fs.customer_id) AS unique_customers
FROM fact_sales fs
INNER JOIN dim_date dd ON fs.order_date_id = dd.date_id
INNER JOIN dim_product dp ON fs.product_id = dp.product_id
GROUP BY dd.year, dd.quarter, dp.category
ORDER BY dd.year, dd.quarter, sales DESC;

ANALYZE TABLE dim_customer;
ANALYZE TABLE dim_product;
ANALYZE TABLE dim_date;
ANALYZE TABLE fact_sales;
    