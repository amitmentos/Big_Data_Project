# Gold Layer ER Diagram

```mermaid
erDiagram
fact_sales {
    string sale_id PK
    string product_id FK
    string customer_id FK
    string campaign_id FK
    date date FK
    decimal quantity
    decimal unit_price
    decimal total_amount
    string channel
    timestamp transaction_time
    timestamp ingestion_time
}

fact_user_activity {
    string event_id PK
    string customer_id FK
    string product_id FK
    date date FK
    string event_type
    string page_url
    string device_type
    timestamp event_time
    decimal time_spent_seconds
    string session_id
}

sales_performance_metrics {
    date date PK
    string channel PK
    decimal total_revenue
    bigint total_transactions
    bigint unique_customers
    decimal avg_transaction_value
    decimal conversion_rate
}

customer_segmentation {
    string customer_id PK
    string value_segment
    string activity_status
    string purchase_diversity
    decimal total_spend
    bigint total_transactions
    decimal avg_transaction_value
    date last_purchase_date
    int days_since_last_purchase
    date segment_date
}

campaign_effectiveness {
    string campaign_id PK
    date date PK
    decimal spend
    decimal revenue
    decimal roi
    bigint conversions
    decimal cost_per_acquisition
    string channel
    string customer_segment
}

customer_product_interactions {
    string product_id PK
    date date PK
    bigint total_views
    bigint total_purchases
    decimal conversion_rate
    decimal total_revenue
    decimal avg_selling_price
    int inventory_turnover
    string top_customer_segment
}

fact_sales ||--o{ customer_segmentation : "customer_id"
fact_sales ||--o{ customer_product_interactions : "product_id"
fact_user_activity ||--o{ customer_segmentation : "customer_id"
fact_user_activity ||--o{ customer_product_interactions : "product_id"

sales_performance_metrics ||--o{ customer_segmentation : "customer_id"
sales_performance_metrics ||--o{ customer_product_interactions : "product_id"
campaign_effectiveness ||--o{ customer_segmentation : "customer_segment"
customer_segmentation ||--o{ customer_product_interactions : "top_customer_segment"
