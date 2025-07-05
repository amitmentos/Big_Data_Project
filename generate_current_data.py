#!/usr/bin/env python3
"""
Generate fresh sample data with current timestamps for bronze layer tables
"""
import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
import os

fake = Faker()

def generate_current_timestamp_data():
    """Generate data with current timestamps"""
    now = datetime.now()
    current_date = now.strftime('%Y-%m-%d')
    
    print(f"🔄 Generating data for date: {current_date}")
    
    # Generate fresh customers with current timestamp
    customers = []
    for i in range(100):
        customer = {
            'customer_id': f"CUST-{i:06d}",
            'customer_name': fake.name(),
            'email': fake.email(),
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'country': fake.country(),
            'membership_tier': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
            'created_at': (now - timedelta(days=random.randint(30, 365))).isoformat(),
            'last_updated': now.isoformat(),
            'ingestion_time': now.isoformat()
        }
        customers.append(customer)
    
    # Generate fresh products with current timestamp
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books']
    brands = ['TechCorp', 'StyleMax', 'HomeComfort', 'SportsPro', 'BookWorld']
    products = []
    for i in range(50):
        category = random.choice(categories)
        product = {
            'product_id': f"PROD-{i:06d}",
            'product_name': f"{fake.catch_phrase()} {category}",
            'category': category,
            'subcategory': f"{category} - {fake.word().title()}",
            'brand': random.choice(brands),
            'base_price': round(random.uniform(10.0, 1000.0), 2),
            'description': fake.text(max_nb_chars=200),
            'is_active': random.choice([True, True, True, False]),
            'last_updated': now.isoformat(),
            'ingestion_time': now.isoformat()
        }
        products.append(product)
    
    # Generate fresh user events with current and recent timestamps
    user_events = []
    for i in range(1000):
        # Mix of events from last 24 hours for realistic stream
        event_time = now - timedelta(hours=random.randint(0, 24))
        event = {
            'event_id': str(uuid.uuid4()),
            'session_id': str(uuid.uuid4()),
            'customer_id': random.choice(customers)['customer_id'],
            'event_type': random.choice(['page_view', 'product_view', 'add_to_cart', 'purchase', 'search']),
            'event_time': event_time.isoformat(),
            'page_url': f"https://ecommerce.com/{fake.uri_path()}",
            'device_type': random.choice(['desktop', 'mobile', 'tablet']),
            'metadata': {
                'product_id': random.choice(products)['product_id'] if random.random() > 0.3 else None,
                'user_agent': fake.user_agent(),
                'ip_address': fake.ipv4()
            },
            'ingestion_time': now.isoformat()
        }
        user_events.append(event)
    
    # Generate fresh marketplace sales with CURRENT DATE and varied delays for demo
    marketplaces = ['Amazon', 'eBay', 'Etsy', 'Walmart', 'Target']
    marketplace_sales = []
    for i in range(500):
        # IMPORTANT: Use current date (June 17, 2025) with varied delays
        # 60% recent (last 3 days), 30% medium delay (3-7 days), 10% old delay (7-14 days)
        delay_choice = random.random()
        if delay_choice < 0.6:  # Recent transactions
            transaction_time = now - timedelta(days=random.uniform(0, 3))
        elif delay_choice < 0.9:  # Medium delay
            transaction_time = now - timedelta(days=random.uniform(3, 7))
        else:  # Older transactions for late arrival demo
            transaction_time = now - timedelta(days=random.uniform(7, 14))
        
        product = random.choice(products)
        sale = {
            'transaction_id': str(uuid.uuid4()),
            'marketplace_name': random.choice(marketplaces),
            'seller_id': str(uuid.uuid4()),
            'product_id': product['product_id'],
            'amount': round(product['base_price'] * random.uniform(0.8, 1.2), 2),
            'currency': 'USD',
            'quantity': random.randint(1, 5),
            'transaction_time': transaction_time.isoformat(),
            'settlement_time': (transaction_time + timedelta(days=random.randint(1, 7))).isoformat(),
            'payment_method': random.choice(['Credit Card', 'PayPal', 'Bank Transfer', 'Digital Wallet']),
            'status': random.choice(['completed', 'completed', 'completed', 'pending', 'cancelled']),
            'marketplace_metadata': {
                'commission_rate': round(random.uniform(0.05, 0.15), 3),
                'shipping_cost': round(random.uniform(5.0, 25.0), 2),
                'tax_amount': round(random.uniform(0.0, 50.0), 2)
            },
            'ingestion_time': now.isoformat(),
            'data_freshness': 'current_date_' + current_date,
            'demo_timestamp': now.isoformat()
        }
        marketplace_sales.append(sale)
    
    # Generate fresh marketing campaigns
    campaigns = []
    for i in range(10):
        start_date = fake.date_between(start_date='-1m', end_date='+1m')
        campaign = {
            'campaign_id': f'CAMP_{i:03d}',
            'campaign_name': f"{fake.catch_phrase()} Campaign",
            'campaign_type': random.choice(['Email', 'Social Media', 'Display', 'Search', 'Influencer']),
            'start_date': start_date.isoformat(),
            'end_date': (start_date + timedelta(days=random.randint(7, 90))).isoformat(),
            'budget': round(random.uniform(1000.0, 100000.0), 2),
            'target_audience': random.choice(['All', 'New Customers', 'VIP', 'Category Specific']),
            'ingestion_time': now.isoformat()
        }
        campaigns.append(campaign)
    
    # Save to files
    os.makedirs('sample_data', exist_ok=True)
    
    data_files = {
        'customers': customers,
        'products': products,
        'user_events': user_events,
        'marketplace_sales': marketplace_sales,
        'campaigns': campaigns
    }
    
    for name, data in data_files.items():
        filepath = f"sample_data/{name}.json"
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        print(f"✅ Generated {len(data)} current {name} records -> {filepath}")
    
    print(f"🎉 Fresh data generation completed with timestamp: {now.isoformat()}")
    return current_date

if __name__ == "__main__":
    generate_current_timestamp_data()
