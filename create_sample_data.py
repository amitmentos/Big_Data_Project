#!/usr/bin/env python3
"""
Sample Data Generator for E-commerce Data Platform
Generates realistic sample data for testing and demonstration
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import os

fake = Faker()

class SampleDataGenerator:
    def __init__(self):
        self.fake = fake
        
        # Product categories and brands
        self.categories = [
            'Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books',
            'Beauty', 'Automotive', 'Toys', 'Health', 'Food'
        ]
        
        self.brands = [
            'TechCorp', 'StyleMax', 'HomeComfort', 'SportsPro', 'BookWorld',
            'BeautyPlus', 'AutoMax', 'PlayTime', 'HealthFirst', 'FoodDelight'
        ]
        
        self.device_types = ['desktop', 'mobile', 'tablet']
        self.event_types = ['page_view', 'product_view', 'add_to_cart', 'purchase', 'search']
        self.marketplaces = ['Amazon', 'eBay', 'Etsy', 'Walmart', 'Target']
        
    def generate_customers(self, num_customers=1000):
        """Generate customer data"""
        customers = []
        
        for _ in range(num_customers):
            customer = {
                'customer_id': str(uuid.uuid4()),
                'customer_name': self.fake.name(),
                'email': self.fake.email(),
                'address': self.fake.address().replace('\n', ', '),
                'city': self.fake.city(),
                'country': self.fake.country(),
                'membership_tier': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
                'created_at': self.fake.date_time_between(start_date='-2y', end_date='now').isoformat(),
                'last_updated': datetime.now().isoformat()
            }
            customers.append(customer)
            
        return customers
    
    def generate_products(self, num_products=500):
        """Generate product catalog data"""
        products = []
        
        for _ in range(num_products):
            category = random.choice(self.categories)
            product = {
                'product_id': str(uuid.uuid4()),
                'product_name': f"{self.fake.catch_phrase()} {category}",
                'category': category,
                'subcategory': f"{category} - {self.fake.word().title()}",
                'brand': random.choice(self.brands),
                'base_price': round(random.uniform(10.0, 1000.0), 2),
                'description': self.fake.text(max_nb_chars=200),
                'is_active': random.choice([True, True, True, False]),  # 75% active
                'last_updated': datetime.now().isoformat()
            }
            products.append(product)
            
        return products
    
    def generate_marketing_campaigns(self, num_campaigns=50):
        """Generate marketing campaign data"""
        campaigns = []
        
        for _ in range(num_campaigns):
            start_date = self.fake.date_between(start_date='-6m', end_date='+1m')
            end_date = start_date + timedelta(days=random.randint(7, 90))
            
            campaign = {
                'campaign_id': str(uuid.uuid4()),
                'campaign_name': f"{self.fake.catch_phrase()} Campaign",
                'campaign_type': random.choice(['Email', 'Social Media', 'Display', 'Search', 'Influencer']),
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'budget': round(random.uniform(1000.0, 100000.0), 2),
                'target_audience': random.choice(['All', 'New Customers', 'VIP', 'Category Specific'])
            }
            campaigns.append(campaign)
            
        return campaigns
    
    def generate_user_events(self, customers, products, num_events=10000):
        """Generate user activity events"""
        events = []
        
        for _ in range(num_events):
            customer = random.choice(customers)
            product = random.choice(products) if random.random() > 0.3 else None
            
            event = {
                'event_id': str(uuid.uuid4()),
                'session_id': str(uuid.uuid4()),
                'customer_id': customer['customer_id'],
                'event_type': random.choice(self.event_types),
                'event_time': self.fake.date_time_between(start_date='-30d', end_date='now').isoformat(),
                'page_url': f"https://ecommerce.com/{self.fake.uri_path()}",
                'device_type': random.choice(self.device_types),
                'metadata': {
                    'product_id': product['product_id'] if product else None,
                    'category': product['category'] if product else None,
                    'user_agent': self.fake.user_agent(),
                    'ip_address': self.fake.ipv4()
                },
                'ingestion_time': datetime.now().isoformat()
            }
            events.append(event)
            
        return events
    
    def generate_marketplace_sales(self, products, num_sales=5000):
        """Generate marketplace sales data"""
        sales = []
        
        for _ in range(num_sales):
            product = random.choice(products)
            transaction_time = self.fake.date_time_between(start_date='-30d', end_date='now')
            
            sale = {
                'transaction_id': str(uuid.uuid4()),
                'marketplace_name': random.choice(self.marketplaces),
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
                'ingestion_time': datetime.now().isoformat()
            }
            sales.append(sale)
            
        return sales
    
    def save_to_files(self, data_dict, output_dir='sample_data'):
        """Save generated data to JSON files"""
        os.makedirs(output_dir, exist_ok=True)
        
        for filename, data in data_dict.items():
            filepath = os.path.join(output_dir, f"{filename}.json")
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            print(f"✅ Saved {len(data)} records to {filepath}")
    
    def generate_all_sample_data(self):
        """Generate all sample data"""
        print("🔄 Generating sample data...")
        
        # Generate base data
        customers = self.generate_customers(1000)
        products = self.generate_products(500)
        campaigns = self.generate_marketing_campaigns(50)
        
        # Generate event data
        user_events = self.generate_user_events(customers, products, 10000)
        marketplace_sales = self.generate_marketplace_sales(products, 5000)
        
        # Save all data
        data_dict = {
            'customers': customers,
            'products': products,
            'campaigns': campaigns,
            'user_events': user_events,
            'marketplace_sales': marketplace_sales
        }
        
        self.save_to_files(data_dict)
        
        print("\n📊 Sample Data Generation Summary:")
        print(f"  • Customers: {len(customers)}")
        print(f"  • Products: {len(products)}")
        print(f"  • Marketing Campaigns: {len(campaigns)}")
        print(f"  • User Events: {len(user_events)}")
        print(f"  • Marketplace Sales: {len(marketplace_sales)}")
        print("\n✅ Sample data generation completed!")

if __name__ == "__main__":
    generator = SampleDataGenerator()
    generator.generate_all_sample_data() 