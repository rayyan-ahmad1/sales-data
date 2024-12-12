import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import datetime

class SalesPerformanceDashboard:
    def __init__(self, db_path='sales_database.db'):
        """
        Initialize sales performance dashboard
        
        Args:
            db_path (str): Path to SQLite database
        """
        self.db_connection = sqlite3.connect(db_path)
        self.data = None
        self.engine = create_engine(f'sqlite:///{db_path}')

    def extract_data(self):
        """
        Extract sales data from multiple sources
        """
        # SQL query to fetch comprehensive sales data
        query = """
        SELECT 
            s.sale_id,
            p.product_name,
            p.category,
            r.region_name,
            r.country,
            s.sale_date,
            s.quantity,
            s.unit_price,
            s.total_revenue,
            c.customer_segment,
            e.employee_id,
            e.sales_rep_name
        FROM 
            sales s
        JOIN 
            products p ON s.product_id = p.product_id
        JOIN 
            regions r ON s.region_id = r.region_id
        JOIN 
            customers c ON s.customer_id = c.customer_id
        JOIN 
            employees e ON s.sales_rep_id = e.employee_id
        """
        
        self.data = pd.read_sql_query(query, self.db_connection)
        
        # Date parsing and feature engineering
        self.data['sale_date'] = pd.to_datetime(self.data['sale_date'])
        self.data['month'] = self.data['sale_date'].dt.to_period('M')
        self.data['total_sale_value'] = self.data['quantity'] * self.data['unit_price']
        
        return self.data

    def calculate_kpis(self):
        """
        Calculate key performance indicators
        """
        kpis = {
            'total_revenue': self.data['total_sale_value'].sum(),
            'total_sales_volume': self.data['quantity'].sum(),
            'average_transaction_value': self.data['total_sale_value'].mean(),
            'unique_customers': self.data['customer_segment'].nunique(),
            'top_performing_region': self.data.groupby('region_name')['total_sale_value'].sum().idxmax()
        }
        
        print("Key Performance Indicators:")
        for key, value in kpis.items():
            print(f"{key.replace('_', ' ').title()}: {value}")
        
        return kpis

    def regional_performance_analysis(self):
        """
        Analyze sales performance by region
        """
        regional_performance = self.data.groupby('region_name').agg({
            'total_sale_value': ['sum', 'mean'],
            'quantity': ['sum', 'count']
        }).reset_index()
        
        regional_performance.columns = ['Region', 'Total Revenue', 'Average Sale', 'Total Units', 'Total Transactions']
        
        # Visualization
        plt.figure(figsize=(12, 6))
        
        # Revenue by Region
        plt.subplot(121)
        sns.barplot(x='Region', y='Total Revenue', data=regional_performance)
        plt.title('Revenue by Region')
        plt.xticks(rotation=45)
        
        # Transactions by Region
        plt.subplot(122)
        sns.barplot(x='Region', y='Total Transactions', data=regional_performance)
        plt.title('Transactions by Region')
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.show()
        
        return regional_performance

    def customer_acquisition_analysis(self):
        """
        Analyze customer acquisition and segmentation
        """
        customer_analysis = self.data.groupby('customer_segment').agg({
            'total_sale_value': ['sum', 'mean'],
            'sale_id': 'count'
        }).reset_index()
        
        customer_analysis.columns = ['Customer Segment', 'Total Revenue', 'Average Sale', 'Total Transactions']
        
        plt.figure(figsize=(10, 5))
        sns.scatterplot(
            x='Total Revenue', 
            y='Average Sale', 
            size='Total Transactions', 
            data=customer_analysis, 
            hue='Customer Segment'
        )
        plt.title('Customer Segment Performance')
        plt.show()
        
        return customer_analysis

    def export_insights(self):
        """
        Export analysis results to CSV
        """
        insights = {
            'regional_performance': self.regional_performance_analysis(),
            'customer_analysis': self.customer_acquisition_analysis()
        }
        
        for name, df in insights.items():
            df.to_csv(f'{name}_insights.csv', index=False)
        
        print("Insights exported successfully!")

def main():
    dashboard = SalesPerformanceDashboard()
    
    # Extract and process data
    sales_data = dashboard.extract_data()
    
    # Calculate KPIs
    kpis = dashboard.calculate_kpis()
    
    # Perform detailed analyses
    dashboard.regional_performance_analysis()
    dashboard.customer_acquisition_analysis()
    
    # Export insights
    dashboard.export_insights()

if __name__ == "__main__":
    main()

# SQL Script for Database Setup (To be run separately)
"""
CREATE TABLE regions (
    region_id INTEGER PRIMARY KEY,
    region_name TEXT,
    country TEXT
);

CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    unit_price DECIMAL(10,2)
);

CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    customer_segment TEXT
);

CREATE TABLE employees (
    employee_id INTEGER PRIMARY KEY,
    sales_rep_name TEXT
);

CREATE TABLE sales (
    sale_id INTEGER PRIMARY KEY,
    product_id INTEGER,
    region_id INTEGER,
    customer_id INTEGER,
    sales_rep_id INTEGER,
    sale_date DATE,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_revenue DECIMAL(10,2),
    FOREIGN KEY(product_id) REFERENCES products(product_id),
    FOREIGN KEY(region_id) REFERENCES regions(region_id),
    FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY(sales_rep_id) REFERENCES employees(employee_id)
);
"""
