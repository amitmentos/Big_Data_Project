# Bronze Layer ER Diagram

```mermaid
erDiagram
RAW_USER_EVENTS {
    string event_id PK
    string session_id
    string customer_id FK
    string event_type
    timestamp event_time
    string page_url
    string product_id FK
    string device_type
    map metadata
    timestamp ingestion_time
    timestamp kafka_timestamp
    int kafka_partition
    bigint kafka_offset
}

RAW_MARKETPLACE_SALES {
    string transaction_id PK
    string marketplace_name
    string seller_id
    string product_id FK
    decimal amount
    string currency
    int quantity
    string status
    timestamp transaction_time
    timestamp settlement_time
    string payment_method
    map marketplace_metadata
    timestamp ingestion_time
    timestamp kafka_timestamp
    int kafka_partition
    bigint kafka_offset
}

RAW_PRODUCT_CATALOG {
    string product_id PK
    string product_name
    string category
    string subcategory
    string brand
    decimal base_price
    string description
    timestamp last_updated
    timestamp ingestion_time
}

RAW_CUSTOMER_DATA {
    string customer_id PK
    string customer_name
    string email
    string address
    string city
    string country
    string membership_tier
    timestamp created_at
    timestamp last_updated
    timestamp ingestion_time
}

RAW_MARKETING_CAMPAIGNS {
    string campaign_id PK
    string campaign_name
    string campaign_type
    date start_date
    date end_date
    decimal budget
    string target_audience
    timestamp ingestion_time
}

RAW_USER_EVENTS ||--o{ RAW_CUSTOMER_DATA : "customer_id"
RAW_USER_EVENTS ||--o{ RAW_PRODUCT_CATALOG : "product_id"
RAW_MARKETPLACE_SALES ||--o{ RAW_PRODUCT_CATALOG : "product_id"
