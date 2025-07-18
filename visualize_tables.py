#!/usr/bin/env python3
"""
E-commerce Data Platform Table Visualization Script
Creates comprehensive visualizations for Bronze, Silver, and Gold layer tables
"""

import os
import sys
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import logging
from builtins import sum as builtin_sum
from builtins import max as builtin_max
from builtins import min as builtin_min

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set up plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    SPARK_AVAILABLE = True
except ImportError:
    logger.warning("PySpark not available. Will use sample data for visualization.")
    SPARK_AVAILABLE = False

class DataPlatformVisualizer:
    def __init__(self, use_spark=True):
        """Initialize the visualizer with optional Spark connection"""
        self.use_spark = use_spark and SPARK_AVAILABLE
        self.spark = None
        
        if self.use_spark:
            self.init_spark_session()
        
        # Create output directory for plots
        self.output_dir = "visualizations"
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Color schemes for different chart types
        self.colors = {
            'primary': '#1f77b4',
            'secondary': '#ff7f0e', 
            'success': '#2ca02c',
            'danger': '#d62728',
            'warning': '#ff9800',
            'info': '#17a2b8',
            'palette': ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', 
                       '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
        }
    
    def init_spark_session(self):
        """Initialize Spark session with Iceberg support"""
        try:
            self.spark = SparkSession.builder \
                .appName("Data Platform Visualizer") \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
                .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
                .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://warehouse/") \
                .config("spark.sql.catalog.spark_catalog.s3.endpoint", "http://minio:9000") \
                .config("spark.hadoop.fs.s3a.access.key", "minio") \
                .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
                .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("ERROR")
            logger.info("Spark session initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {e}")
            self.use_spark = False
    
    def query_table(self, table_name, limit=10000):
        """Query a table and return as pandas DataFrame"""
        if not self.use_spark:
            return self.generate_sample_data(table_name, limit)
        
        try:
            df = self.spark.sql(f"SELECT * FROM {table_name} LIMIT {limit}")
            return df.toPandas()
        except Exception as e:
            logger.error(f"Error querying {table_name}: {e}")
            return self.generate_sample_data(table_name, limit)
    
    def generate_sample_data(self, table_name, limit):
        """Generate sample data when Spark is not available"""
        logger.info(f"Generating sample data for {table_name}")
        
        # Generate sample data based on table type
        if 'user_events' in table_name or 'user_activity' in table_name:
            return self.generate_user_events_sample(limit)
        elif 'sales' in table_name and 'performance' not in table_name:
            return self.generate_sales_sample(limit)
        elif 'customer' in table_name:
            return self.generate_customer_sample(limit)
        elif 'product' in table_name:
            return self.generate_product_sample(limit)
        elif 'performance' in table_name:
            return self.generate_performance_sample(limit)
        elif 'campaign' in table_name:
            return self.generate_campaign_sample(limit)
        else:
            return pd.DataFrame()
    
    def generate_user_events_sample(self, n=1000):
        """Generate sample user events data"""
        np.random.seed(42)
        return pd.DataFrame({
            'event_id': [f'evt_{i}' for i in range(n)],
            'customer_id': [f'cust_{np.random.randint(1, 500)}' for _ in range(n)],
            'event_type': np.random.choice(['page_view', 'product_view', 'add_to_cart', 'purchase', 'search'], n),
            'device_type': np.random.choice(['desktop', 'mobile', 'tablet'], n),
            'event_time': pd.date_range(start='2024-01-01', periods=n, freq='H'),
            'time_spent_seconds': np.random.exponential(30, n),
            'product_id': [f'prod_{np.random.randint(1, 100)}' if np.random.random() > 0.3 else None for _ in range(n)]
        })
    
    def generate_sales_sample(self, n=500):
        """Generate sample sales data"""
        np.random.seed(42)
        return pd.DataFrame({
            'customer_id': [f'cust_{np.random.randint(1, 200)}' for _ in range(n)],
            'product_id': [f'prod_{np.random.randint(1, 100)}' for _ in range(n)],
            'channel': np.random.choice(['website', 'mobile_app', 'marketplace'], n),
            'total_amount': np.random.gamma(2, 50, n),
            'quantity': np.random.poisson(2, n) + 1,
            'transaction_time': pd.date_range(start='2024-01-01', periods=n, freq='2H'),
            'currency': ['USD'] * n
        })
    
    def generate_customer_sample(self, n=300):
        """Generate sample customer data"""
        np.random.seed(42)
        return pd.DataFrame({
            'customer_id': [f'cust_{i}' for i in range(n)],
            'membership_tier': np.random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'], n, p=[0.4, 0.3, 0.2, 0.1]),
            'total_spend': np.random.gamma(2, 200, n),
            'total_transactions': np.random.poisson(10, n) + 1,
            'days_since_last_purchase': np.random.exponential(30, n),
            'value_segment': np.random.choice(['High Value', 'Medium Value', 'Low Value'], n),
            'activity_status': np.random.choice(['Active', 'Inactive', 'At Risk'], n)
        })
    
    def generate_product_sample(self, n=100):
        """Generate sample product data"""
        np.random.seed(42)
        categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books']
        return pd.DataFrame({
            'product_id': [f'prod_{i}' for i in range(n)],
            'category': np.random.choice(categories, n),
            'brand': [f'Brand_{np.random.randint(1, 20)}' for _ in range(n)],
            'base_price': np.random.gamma(2, 50, n),
            'is_active': np.random.choice([True, False], n, p=[0.8, 0.2])
        })
    
    def generate_performance_sample(self, n=30):
        """Generate sample performance metrics"""
        np.random.seed(42)
        return pd.DataFrame({
            'date_id': pd.date_range(start='2024-01-01', periods=n, freq='D'),
            'channel': np.tile(['website', 'mobile_app', 'marketplace'], n // 3 + 1)[:n],
            'total_revenue': np.random.gamma(3, 1000, n),
            'total_transactions': np.random.poisson(100, n) + 50,
            'unique_customers': np.random.poisson(80, n) + 30,
            'conversion_rate': np.random.beta(2, 20, n)
        })
    
    def generate_campaign_sample(self, n=20):
        """Generate sample campaign data"""
        np.random.seed(42)
        return pd.DataFrame({
            'campaign_id': [f'camp_{i}' for i in range(n)],
            'channel': np.random.choice(['Email', 'Social Media', 'Display', 'Search'], n),
            'spend': np.random.gamma(2, 5000, n),
            'revenue': np.random.gamma(3, 8000, n),
            'conversions': np.random.poisson(50, n) + 10,
            'date_id': pd.date_range(start='2024-01-01', periods=n, freq='D')
        })
    
    def plot_user_activity_trends(self):
        """Plot user activity trends and patterns"""
        logger.info("Creating user activity visualizations...")
        
        # Get user events data
        events_df = self.query_table("gold.fact_user_activity", 5000)
        if events_df.empty:
            events_df = self.generate_user_events_sample(2000)
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('User Activity Analysis', fontsize=16, fontweight='bold')
        
        # Event types distribution
        event_counts = events_df['event_type'].value_counts()
        axes[0,0].pie(event_counts.values, labels=event_counts.index, autopct='%1.1f%%', 
                     colors=self.colors['palette'][:len(event_counts)])
        axes[0,0].set_title('Event Types Distribution')
        
        # Device type analysis
        device_counts = events_df['device_type'].value_counts()
        sns.barplot(x=device_counts.index, y=device_counts.values, ax=axes[0,1], 
                   palette=self.colors['palette'])
        axes[0,1].set_title('Events by Device Type')
        axes[0,1].set_ylabel('Number of Events')
        
        # Hourly activity pattern
        if 'event_time' in events_df.columns:
            events_df['event_time'] = pd.to_datetime(events_df['event_time'])
            events_df['hour'] = events_df['event_time'].dt.hour
            hourly_activity = events_df['hour'].value_counts().sort_index()
            axes[1,0].plot(hourly_activity.index, hourly_activity.values, 
                          marker='o', color=self.colors['primary'])
            axes[1,0].set_title('User Activity by Hour of Day')
            axes[1,0].set_xlabel('Hour')
            axes[1,0].set_ylabel('Number of Events')
            axes[1,0].grid(True, alpha=0.3)
        
        # Time spent analysis
        if 'time_spent_seconds' in events_df.columns:
            events_df['time_spent_seconds'] = pd.to_numeric(events_df['time_spent_seconds'], errors='coerce')
            axes[1,1].hist(events_df['time_spent_seconds'].dropna(), bins=30, 
                          color=self.colors['secondary'], alpha=0.7)
            axes[1,1].set_title('Time Spent Distribution')
            axes[1,1].set_xlabel('Time Spent (seconds)')
            axes[1,1].set_ylabel('Frequency')
        
        plt.tight_layout()
        plt.savefig(f'{self.output_dir}/user_activity_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def plot_sales_performance(self):
        """Plot sales performance metrics and trends"""
        logger.info("Creating sales performance visualizations...")
        
        # Get sales performance data
        perf_df = self.query_table("gold.sales_performance_metrics", 1000)
        if perf_df.empty:
            perf_df = self.generate_performance_sample(90)
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Sales Performance Dashboard', fontsize=16, fontweight='bold')
        
        # Revenue trends by channel
        if 'date_id' in perf_df.columns:
            perf_df['date_id'] = pd.to_datetime(perf_df['date_id'])
            channel_revenue = perf_df.groupby(['date_id', 'channel'])['total_revenue'].sum().unstack(fill_value=0)
            
            for i, channel in enumerate(channel_revenue.columns):
                axes[0,0].plot(channel_revenue.index, channel_revenue[channel], 
                              marker='o', label=channel, color=self.colors['palette'][i])
            axes[0,0].set_title('Revenue Trends by Channel')
            axes[0,0].set_xlabel('Date')
            axes[0,0].set_ylabel('Total Revenue')
            axes[0,0].legend()
            axes[0,0].grid(True, alpha=0.3)
        
        # Channel performance comparison
        channel_totals = perf_df.groupby('channel')['total_revenue'].sum().sort_values(ascending=False)
        sns.barplot(x=channel_totals.values, y=channel_totals.index, ax=axes[0,1], 
                   palette=self.colors['palette'])
        axes[0,1].set_title('Total Revenue by Channel')
        axes[0,1].set_xlabel('Total Revenue')
        
        # Conversion rate analysis
        if 'conversion_rate' in perf_df.columns:
            conv_data = perf_df.groupby('channel')['conversion_rate'].agg(['mean', 'std']).reset_index()
            axes[1,0].bar(conv_data['channel'], conv_data['mean'], 
                         yerr=conv_data['std'], capsize=5, color=self.colors['success'], alpha=0.7)
            axes[1,0].set_title('Average Conversion Rate by Channel')
            axes[1,0].set_ylabel('Conversion Rate')
            axes[1,0].tick_params(axis='x', rotation=45)
        
        # Transaction volume vs Revenue scatter
        if 'total_transactions' in perf_df.columns:
            scatter = axes[1,1].scatter(perf_df['total_transactions'], perf_df['total_revenue'], 
                                      c=perf_df['channel'].astype('category').cat.codes, 
                                      cmap='viridis', alpha=0.6)
            axes[1,1].set_title('Transaction Volume vs Revenue')
            axes[1,1].set_xlabel('Total Transactions')
            axes[1,1].set_ylabel('Total Revenue')
            
            # Add trend line
            z = np.polyfit(perf_df['total_transactions'], perf_df['total_revenue'], 1)
            p = np.poly1d(z)
            axes[1,1].plot(perf_df['total_transactions'], p(perf_df['total_transactions']), 
                          "r--", alpha=0.8)
        
        plt.tight_layout()
        plt.savefig(f'{self.output_dir}/sales_performance_dashboard.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def plot_customer_segmentation(self):
        """Plot customer segmentation analysis"""
        logger.info("Creating customer segmentation visualizations...")
        
        # Get customer segmentation data
        cust_df = self.query_table("gold.customer_segmentation", 2000)
        if cust_df.empty:
            cust_df = self.generate_customer_sample(500)
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Customer Segmentation Analysis', fontsize=16, fontweight='bold')
        
        # Value segment distribution
        if 'value_segment' in cust_df.columns:
            segment_counts = cust_df['value_segment'].value_counts()
            axes[0,0].pie(segment_counts.values, labels=segment_counts.index, autopct='%1.1f%%',
                         colors=self.colors['palette'][:len(segment_counts)])
            axes[0,0].set_title('Customer Value Segments')
        
        # Membership tier analysis
        if 'membership_tier' in cust_df.columns:
            tier_revenue = cust_df.groupby('membership_tier')['total_spend'].agg(['sum', 'mean']).reset_index()
            x = np.arange(len(tier_revenue))
            
            axes[0,1].bar(x - 0.2, tier_revenue['sum'], 0.4, label='Total Spend', 
                         color=self.colors['primary'])
            ax2 = axes[0,1].twinx()
            ax2.bar(x + 0.2, tier_revenue['mean'], 0.4, label='Avg Spend', 
                   color=self.colors['secondary'])
            
            axes[0,1].set_xlabel('Membership Tier')
            axes[0,1].set_ylabel('Total Spend', color=self.colors['primary'])
            ax2.set_ylabel('Average Spend', color=self.colors['secondary'])
            axes[0,1].set_xticks(x)
            axes[0,1].set_xticklabels(tier_revenue['membership_tier'])
            axes[0,1].set_title('Revenue by Membership Tier')
        
        # Customer activity status
        if 'activity_status' in cust_df.columns:
            status_counts = cust_df['activity_status'].value_counts()
            colors_map = {'Active': self.colors['success'], 'Inactive': self.colors['danger'], 
                         'At Risk': self.colors['warning']}
            bar_colors = [colors_map.get(status, self.colors['info']) for status in status_counts.index]
            
            axes[1,0].bar(status_counts.index, status_counts.values, color=bar_colors)
            axes[1,0].set_title('Customer Activity Status')
            axes[1,0].set_ylabel('Number of Customers')
        
        # RFM Analysis - Total Spend vs Transaction Frequency
        if 'total_spend' in cust_df.columns and 'total_transactions' in cust_df.columns:
            scatter = axes[1,1].scatter(cust_df['total_transactions'], cust_df['total_spend'],
                                      alpha=0.6, color=self.colors['primary'])
            axes[1,1].set_xlabel('Total Transactions')
            axes[1,1].set_ylabel('Total Spend')
            axes[1,1].set_title('Customer Value Distribution')
            
            # Add quadrant lines
            med_transactions = cust_df['total_transactions'].median()
            med_spend = cust_df['total_spend'].median()
            axes[1,1].axvline(med_transactions, color='red', linestyle='--', alpha=0.5)
            axes[1,1].axhline(med_spend, color='red', linestyle='--', alpha=0.5)
        
        plt.tight_layout()
        plt.savefig(f'{self.output_dir}/customer_segmentation_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def plot_product_performance(self):
        """Plot product and category performance"""
        logger.info("Creating product performance visualizations...")
        
        # Get product and sales data
        product_df = self.query_table("silver.dim_product", 1000)
        sales_df = self.query_table("gold.fact_sales", 2000)
        
        if product_df.empty:
            product_df = self.generate_product_sample(200)
        if sales_df.empty:
            sales_df = self.generate_sales_sample(1000)
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Product Performance Analysis', fontsize=16, fontweight='bold')
        
        # Category performance
        if 'category' in product_df.columns:
            # Merge with sales data if possible
            if 'product_id' in sales_df.columns and 'total_amount' in sales_df.columns:
                merged_df = sales_df.merge(product_df[['product_id', 'category']], 
                                         on='product_id', how='left')
                cat_revenue = merged_df.groupby('category')['total_amount'].sum().sort_values(ascending=False)
            else:
                cat_counts = product_df['category'].value_counts()
                cat_revenue = cat_counts
            
            axes[0,0].bar(range(len(cat_revenue)), cat_revenue.values, 
                         color=self.colors['palette'][:len(cat_revenue)])
            axes[0,0].set_xticks(range(len(cat_revenue)))
            axes[0,0].set_xticklabels(cat_revenue.index, rotation=45)
            axes[0,0].set_title('Performance by Category')
            axes[0,0].set_ylabel('Revenue' if 'total_amount' in sales_df.columns else 'Product Count')
        
        # Price distribution by category
        if 'base_price' in product_df.columns and 'category' in product_df.columns:
            categories = product_df['category'].unique()
            for i, cat in enumerate(categories[:5]):  # Top 5 categories
                cat_data = product_df[product_df['category'] == cat]['base_price']
                axes[0,1].hist(cat_data, alpha=0.6, label=cat, bins=20, 
                              color=self.colors['palette'][i])
            axes[0,1].set_title('Price Distribution by Category')
            axes[0,1].set_xlabel('Base Price')
            axes[0,1].set_ylabel('Frequency')
            axes[0,1].legend()
        
        # Brand performance
        if 'brand' in product_df.columns:
            brand_counts = product_df['brand'].value_counts().head(10)
            axes[1,0].barh(range(len(brand_counts)), brand_counts.values, 
                          color=self.colors['secondary'])
            axes[1,0].set_yticks(range(len(brand_counts)))
            axes[1,0].set_yticklabels(brand_counts.index)
            axes[1,0].set_title('Top 10 Brands by Product Count')
            axes[1,0].set_xlabel('Number of Products')
        
        # Active vs Inactive products
        if 'is_active' in product_df.columns:
            active_counts = product_df['is_active'].value_counts()
            labels = ['Active', 'Inactive']
            colors = [self.colors['success'], self.colors['danger']]
            axes[1,1].pie(active_counts.values, labels=labels, autopct='%1.1f%%', 
                         colors=colors)
            axes[1,1].set_title('Product Catalog Status')
        
        plt.tight_layout()
        plt.savefig(f'{self.output_dir}/product_performance_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def plot_campaign_effectiveness(self):
        """Plot marketing campaign effectiveness"""
        logger.info("Creating campaign effectiveness visualizations...")
        
        # Get campaign data
        campaign_df = self.query_table("gold.campaign_effectiveness", 500)
        if campaign_df.empty:
            campaign_df = self.generate_campaign_sample(50)
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Marketing Campaign Effectiveness', fontsize=16, fontweight='bold')
        
        # ROI by channel
        if 'spend' in campaign_df.columns and 'revenue' in campaign_df.columns:
            campaign_df['roi'] = (campaign_df['revenue'] - campaign_df['spend']) / campaign_df['spend'] * 100
            
            if 'channel' in campaign_df.columns:
                roi_by_channel = campaign_df.groupby('channel')['roi'].mean().sort_values(ascending=False)
                colors = [self.colors['success'] if roi > 0 else self.colors['danger'] for roi in roi_by_channel.values]
                axes[0,0].bar(roi_by_channel.index, roi_by_channel.values, color=colors)
                axes[0,0].set_title('Average ROI by Channel')
                axes[0,0].set_ylabel('ROI (%)')
                axes[0,0].tick_params(axis='x', rotation=45)
                axes[0,0].axhline(y=0, color='black', linestyle='-', alpha=0.3)
        
        # Spend vs Revenue scatter
        if 'spend' in campaign_df.columns and 'revenue' in campaign_df.columns:
            scatter = axes[0,1].scatter(campaign_df['spend'], campaign_df['revenue'], 
                                      alpha=0.6, color=self.colors['primary'])
            axes[0,1].set_xlabel('Campaign Spend')
            axes[0,1].set_ylabel('Campaign Revenue')
            axes[0,1].set_title('Spend vs Revenue')
            
            # Add break-even line
            max_val = builtin_max(campaign_df['spend'].max(), campaign_df['revenue'].max())
            axes[0,1].plot([0, max_val], [0, max_val], 'r--', alpha=0.5, label='Break-even')
            axes[0,1].legend()
        
        # Campaign performance over time
        if 'date_id' in campaign_df.columns:
            campaign_df['date_id'] = pd.to_datetime(campaign_df['date_id'])
            daily_performance = campaign_df.groupby('date_id').agg({
                'spend': 'sum',
                'revenue': 'sum',
                'conversions': 'sum'
            }).reset_index()
            
            axes[1,0].plot(daily_performance['date_id'], daily_performance['revenue'], 
                          marker='o', label='Revenue', color=self.colors['success'])
            axes[1,0].plot(daily_performance['date_id'], daily_performance['spend'], 
                          marker='s', label='Spend', color=self.colors['danger'])
            axes[1,0].set_title('Campaign Performance Over Time')
            axes[1,0].set_xlabel('Date')
            axes[1,0].set_ylabel('Amount')
            axes[1,0].legend()
            axes[1,0].grid(True, alpha=0.3)
        
        # Conversion efficiency
        if 'conversions' in campaign_df.columns and 'spend' in campaign_df.columns:
            campaign_df['cost_per_conversion'] = campaign_df['spend'] / campaign_df['conversions']
            
            if 'channel' in campaign_df.columns:
                cpc_by_channel = campaign_df.groupby('channel')['cost_per_conversion'].mean().sort_values()
                axes[1,1].bar(cpc_by_channel.index, cpc_by_channel.values, 
                             color=self.colors['info'])
                axes[1,1].set_title('Cost per Conversion by Channel')
                axes[1,1].set_ylabel('Cost per Conversion')
                axes[1,1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig(f'{self.output_dir}/campaign_effectiveness_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def create_interactive_dashboard(self):
        """Create an interactive Plotly dashboard"""
        logger.info("Creating interactive dashboard...")
        
        # Get data for dashboard
        perf_df = self.query_table("gold.sales_performance_metrics", 1000)
        if perf_df.empty:
            perf_df = self.generate_performance_sample(90)
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Revenue Trends', 'Channel Performance', 
                          'Transaction Volume', 'Conversion Rates'),
            specs=[[{"secondary_y": False}, {"type": "bar"}],
                   [{"type": "scatter"}, {"type": "bar"}]]
        )
        
        # Revenue trends
        if 'date_id' in perf_df.columns:
            perf_df['date_id'] = pd.to_datetime(perf_df['date_id'])
            channels = perf_df['channel'].unique()
            
            for i, channel in enumerate(channels):
                channel_data = perf_df[perf_df['channel'] == channel]
                fig.add_trace(
                    go.Scatter(x=channel_data['date_id'], y=channel_data['total_revenue'],
                              mode='lines+markers', name=channel,
                              line=dict(color=self.colors['palette'][i % len(self.colors['palette'])])),
                    row=1, col=1
                )
        
        # Channel performance
        channel_totals = perf_df.groupby('channel')['total_revenue'].sum()
        fig.add_trace(
            go.Bar(x=channel_totals.index, y=channel_totals.values,
                  marker_color=self.colors['palette'][:len(channel_totals)],
                  showlegend=False),
            row=1, col=2
        )
        
        # Transaction volume scatter
        if 'total_transactions' in perf_df.columns:
            fig.add_trace(
                go.Scatter(x=perf_df['total_transactions'], y=perf_df['total_revenue'],
                          mode='markers', text=perf_df['channel'],
                          marker=dict(size=8, color=perf_df['channel'].astype('category').cat.codes,
                                    colorscale='viridis'),
                          showlegend=False),
                row=2, col=1
            )
        
        # Conversion rates
        if 'conversion_rate' in perf_df.columns:
            conv_rates = perf_df.groupby('channel')['conversion_rate'].mean()
            fig.add_trace(
                go.Bar(x=conv_rates.index, y=conv_rates.values,
                      marker_color=self.colors['success'],
                      showlegend=False),
                row=2, col=2
            )
        
        # Update layout
        fig.update_layout(
            height=800,
            title_text="E-commerce Data Platform - Interactive Dashboard",
            title_x=0.5,
            showlegend=True
        )
        
        # Save and show
        fig.write_html(f'{self.output_dir}/interactive_dashboard.html')
        fig.show()
    
    def generate_data_summary_report(self):
        """Generate a comprehensive data summary report"""
        logger.info("Generating data summary report...")
        
        # Get table information
        tables_info = {}
        
        table_list = [
            "bronze.raw_user_events",
            "bronze.raw_marketplace_sales", 
            "silver.standardized_user_events",
            "silver.standardized_sales",
            "silver.dim_customer_scd",
            "silver.dim_product",
            "gold.fact_sales",
            "gold.fact_user_activity",
            "gold.sales_performance_metrics",
            "gold.customer_segmentation",
            "gold.campaign_effectiveness"
        ]
        
        print("\n" + "="*80)
        print("E-COMMERCE DATA PLATFORM - TABLE SUMMARY REPORT")
        print("="*80)
        
        for table in table_list:
            try:
                if self.use_spark:
                    # Check if table exists first
                    try:
                        self.spark.sql(f"DESCRIBE TABLE {table}")
                        count_df = self.spark.sql(f"SELECT COUNT(*) as count FROM {table}")
                        record_count = count_df.collect()[0]['count']
                    except Exception:
                        # Table doesn't exist
                        record_count = 0
                        tables_info[table] = record_count
                        layer = table.split('.')[0].upper()
                        table_name = table.split('.')[1]
                        print(f"⚠️  {layer:<8} | {table_name:<30} | Table not found")
                        continue
                else:
                    # Use sample data counts
                    sample_df = self.generate_sample_data(table, 1000)
                    record_count = len(sample_df) if not sample_df.empty else 0
                
                tables_info[table] = record_count
                layer = table.split('.')[0].upper()
                table_name = table.split('.')[1]
                status = "📊" if record_count > 0 else "⚪"
                print(f"{status} {layer:<8} | {table_name:<30} | {record_count:>10,} records")
                
            except Exception as e:
                print(f"❌ {table:<45} | Error: {str(e)[:50]}...")
                tables_info[table] = 0
        
        # Summary statistics
        total_records = builtin_sum(tables_info.values())
        bronze_records = builtin_sum(v for k, v in tables_info.items() if k.startswith('bronze'))
        silver_records = builtin_sum(v for k, v in tables_info.items() if k.startswith('silver'))
        gold_records = builtin_sum(v for k, v in tables_info.items() if k.startswith('gold'))
        
        print("\n" + "-"*80)
        print("LAYER SUMMARY:")
        print(f"🥉 Bronze Layer:  {bronze_records:>12,} records")
        print(f"🥈 Silver Layer:  {silver_records:>12,} records") 
        print(f"🥇 Gold Layer:    {gold_records:>12,} records")
        print(f"📈 Total Records: {total_records:>12,} records")
        
        if total_records == 0:
            print("\n💡 NOTE: No data found in platform tables.")
            print("   The visualizations will use realistic sample data for demonstration.")
        
        print("-"*80)
        
        return tables_info
    
    def run_all_visualizations(self):
        """Run all visualization functions"""
        logger.info("Starting comprehensive visualization generation...")
        
        print("\n🎨 GENERATING E-COMMERCE DATA PLATFORM VISUALIZATIONS 🎨")
        print("="*70)
        
        try:
            # Generate data summary
            self.generate_data_summary_report()
            
            # Create all visualizations
            self.plot_user_activity_trends()
            self.plot_sales_performance()
            self.plot_customer_segmentation()
            self.plot_product_performance()
            self.plot_campaign_effectiveness()
            self.create_interactive_dashboard()
            
            print(f"\n✅ All visualizations completed successfully!")
            print(f"📁 Output directory: {os.path.abspath(self.output_dir)}")
            print(f"📊 Generated files:")
            for file in os.listdir(self.output_dir):
                print(f"   • {file}")
            
        except Exception as e:
            logger.error(f"Error during visualization generation: {e}")
            raise
        
        finally:
            if self.spark:
                self.spark.stop()


def main():
    """Main function to run the visualizer"""
    import argparse
    
    parser = argparse.ArgumentParser(description='E-commerce Data Platform Visualizer')
    parser.add_argument('--no-spark', action='store_true', 
                       help='Use sample data instead of connecting to Spark')
    parser.add_argument('--output-dir', default='visualizations',
                       help='Output directory for plots')
    
    args = parser.parse_args()
    
    # Create visualizer
    visualizer = DataPlatformVisualizer(use_spark=not args.no_spark)
    visualizer.output_dir = args.output_dir
    
    # Run all visualizations
    visualizer.run_all_visualizations()


if __name__ == "__main__":
    main() 