-- =====================================================
-- KPI 1: Revenue Total y Crecimiento
-- =====================================================
WITH monthly_revenue AS (
    SELECT 
        Year,
        Month,
        SUM(Total_Amount) AS Revenue,
        LAG(SUM(Total_Amount)) OVER (ORDER BY Year, Month) AS Prev_Revenue
    FROM silver.retail_sales
    GROUP BY Year, Month
)
SELECT 
    Year,
    Month,
    Revenue,
    Prev_Revenue,
    ROUND(((Revenue - Prev_Revenue) / Prev_Revenue) * 100, 2) AS Growth_Percent
FROM monthly_revenue
ORDER BY Year DESC, Month DESC;

-- =====================================================
-- KPI 2: Top 5 Categorías por Ingresos
-- =====================================================
SELECT 
    Product_Category,
    SUM(Total_Amount) AS Total_Revenue,
    SUM(Quantity) AS Total_Units,
    COUNT(DISTINCT Customer_ID) AS Unique_Customers,
    ROUND(AVG(Total_Amount), 2) AS Avg_Order_Value
FROM silver.retail_sales
GROUP BY Product_Category
ORDER BY Total_Revenue DESC
LIMIT 5;

-- =====================================================
-- KPI 3: Distribución por Edad y Género
-- =====================================================
SELECT 
    AgeGroup,
    Gender,
    COUNT(DISTINCT Customer_ID) AS Customer_Count,
    SUM(Total_Amount) AS Total_Revenue,
    ROUND(AVG(Total_Amount), 2) AS Avg_Spend
FROM silver.retail_sales
GROUP BY AgeGroup, Gender
ORDER BY AgeGroup, Gender;

-- =====================================================
-- KPI 4: Ticket Promedio (AOV) Mensual
-- =====================================================
SELECT 
    Year,
    Month,
    COUNT(Transaction_ID) AS Transactions,
    SUM(Total_Amount) AS Revenue,
    ROUND(AVG(Total_Amount), 2) AS Avg_Order_Value,
    ROUND(SUM(Quantity) / COUNT(Transaction_ID), 2) AS Avg_Items_Per_Order
FROM silver.retail_sales
GROUP BY Year, Month
ORDER BY Year, Month;

-- =====================================================
-- KPI 5: Estacionalidad - Ventas por Día de Semana
-- =====================================================
SELECT 
    CASE DayOfWeek
        WHEN 1 THEN 'Domingo'
        WHEN 2 THEN 'Lunes'
        WHEN 3 THEN 'Martes'
        WHEN 4 THEN 'Miércoles'
        WHEN 5 THEN 'Jueves'
        WHEN 6 THEN 'Viernes'
        WHEN 7 THEN 'Sábado'
    END AS Day_Name,
    COUNT(Transaction_ID) AS Transactions,
    SUM(Total_Amount) AS Revenue,
    ROUND(AVG(Total_Amount), 2) AS Avg_Order_Value
FROM silver.retail_sales
GROUP BY DayOfWeek
ORDER BY DayOfWeek;

-- =====================================================
-- KPI 6: Customer Lifetime Value (CLV) por Segmento
-- =====================================================
SELECT 
    AgeGroup,
    Gender,
    COUNT(DISTINCT Customer_ID) AS Customers,
    ROUND(AVG(Total_Spent), 2) AS Avg_CLV,
    ROUND(AVG(Purchase_Count), 2) AS Avg_Purchases
FROM gold.customer_metrics
GROUP BY AgeGroup, Gender
ORDER BY Avg_CLV DESC;