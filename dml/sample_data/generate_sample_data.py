#!/usr/bin/env python3
"""
RetailWorks Sample Data Generator
Generates realistic sample data for the RetailWorks Snowflake database
Version: 1.0
Date: 2025-07-19
"""

import random
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
import uuid
import numpy as np

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

class RetailWorksDataGenerator:
    def __init__(self):
        self.fake = fake
        
        # Data volume configuration (as per requirements)
        self.num_customers = 50000
        self.num_products = 5000
        self.num_orders = 500000
        self.num_employees = 1000
        
        # Business logic configuration
        self.start_date = datetime.now() - timedelta(days=5*365)  # 5 years of data
        self.end_date = datetime.now()
        
        # Storage for generated data
        self.customers = []
        self.products = []
        self.orders = []
        self.order_items = []
        self.employees = []
        
    def generate_customer_segments(self):
        """Generate customer segments"""
        segments = [
            {'name': 'Enterprise', 'min_revenue': 1000000, 'max_revenue': None, 'discount': 0.15},
            {'name': 'Corporate', 'min_revenue': 100000, 'max_revenue': 999999, 'discount': 0.10},
            {'name': 'Small Business', 'min_revenue': 10000, 'max_revenue': 99999, 'discount': 0.05},
            {'name': 'Individual', 'min_revenue': 0, 'max_revenue': 9999, 'discount': 0.00},
            {'name': 'VIP', 'min_revenue': 50000, 'max_revenue': None, 'discount': 0.20},
            {'name': 'Student', 'min_revenue': 0, 'max_revenue': 5000, 'discount': 0.10}
        ]
        
        data = []
        for i, segment in enumerate(segments, 1):
            data.append({
                'SEGMENT_ID': i,
                'SEGMENT_NAME': segment['name'],
                'DESCRIPTION': f'{segment["name"]} customer segment',
                'MIN_ANNUAL_REVENUE': segment['min_revenue'],
                'MAX_ANNUAL_REVENUE': segment['max_revenue'],
                'DISCOUNT_RATE': segment['discount']
            })
        
        return pd.DataFrame(data)
    
    def generate_categories(self):
        """Generate product categories"""
        categories = [
            'Electronics', 'Computers', 'Home & Garden', 'Sports & Outdoors',
            'Books', 'Clothing', 'Automotive', 'Health & Beauty',
            'Toys & Games', 'Grocery', 'Office Supplies', 'Music & Movies'
        ]
        
        data = []
        for i, category in enumerate(categories, 1):
            data.append({
                'CATEGORY_ID': i,
                'CATEGORY_NAME': category,
                'DESCRIPTION': f'{category} products and accessories'
            })
        
        return pd.DataFrame(data)
    
    def generate_suppliers(self):
        """Generate suppliers"""
        data = []
        
        for i in range(1, 201):  # 200 suppliers
            data.append({
                'SUPPLIER_ID': i,
                'SUPPLIER_NAME': self.fake.company(),
                'CONTACT_NAME': self.fake.name(),
                'CONTACT_TITLE': random.choice(['Sales Manager', 'Account Manager', 'Director', 'VP Sales']),
                'ADDRESS': self.fake.street_address(),
                'CITY': self.fake.city(),
                'REGION': self.fake.state(),
                'POSTAL_CODE': self.fake.postcode(),
                'COUNTRY': self.fake.country(),
                'PHONE': self.fake.phone_number(),
                'EMAIL': self.fake.email(),
                'WEBSITE': self.fake.url(),
                'STATUS': random.choice(['ACTIVE', 'INACTIVE'], weights=[0.9, 0.1]),
                'RATING': round(random.uniform(3.0, 5.0), 1)
            })
        
        return pd.DataFrame(data)
    
    def generate_products(self):
        """Generate products"""
        categories = list(range(1, 13))  # 12 categories
        suppliers = list(range(1, 201))  # 200 suppliers
        colors = ['Red', 'Blue', 'Green', 'Black', 'White', 'Silver', 'Gold', 'Brown']
        sizes = ['XS', 'S', 'M', 'L', 'XL', 'XXL', 'One Size']
        product_lines = ['Standard', 'Premium', 'Economy', 'Professional']
        
        data = []
        
        for i in range(1, self.num_products + 1):
            cost = round(random.uniform(5, 500), 2)
            unit_price = round(cost * random.uniform(1.5, 3.0), 2)
            list_price = round(unit_price * random.uniform(1.1, 1.5), 2)
            
            data.append({
                'PRODUCT_ID': i,
                'PRODUCT_NAME': self.fake.catch_phrase(),
                'PRODUCT_NUMBER': f'P{i:06d}',
                'CATEGORY_ID': random.choice(categories),
                'SUPPLIER_ID': random.choice(suppliers),
                'DESCRIPTION': self.fake.text(max_nb_chars=200),
                'COLOR': random.choice(colors + [None]),
                'SIZE': random.choice(sizes + [None]),
                'WEIGHT': round(random.uniform(0.1, 50.0), 2),
                'UNIT_PRICE': unit_price,
                'COST': cost,
                'LIST_PRICE': list_price,
                'DISCONTINUED': random.choice([True, False], weights=[0.1, 0.9]),
                'REORDER_LEVEL': random.randint(5, 50),
                'UNITS_IN_STOCK': random.randint(0, 1000),
                'UNITS_ON_ORDER': random.randint(0, 100),
                'PRODUCT_LINE': random.choice(product_lines),
                'CLASS': random.choice(['A', 'B', 'C']),
                'STYLE': random.choice(['Classic', 'Modern', 'Vintage', 'Sport'])
            })
        
        return pd.DataFrame(data)
    
    def generate_addresses(self):
        """Generate addresses"""
        data = []
        
        # Generate 75,000 addresses (customers will reference these)
        for i in range(1, 75001):
            data.append({
                'ADDRESS_ID': i,
                'ADDRESS_LINE_1': self.fake.street_address(),
                'ADDRESS_LINE_2': self.fake.secondary_address() if random.random() < 0.3 else None,
                'CITY': self.fake.city(),
                'STATE_PROVINCE': self.fake.state(),
                'POSTAL_CODE': self.fake.postcode(),
                'COUNTRY': self.fake.country(),
                'ADDRESS_TYPE': random.choice(['BILLING', 'SHIPPING', 'BOTH']),
                'LATITUDE': float(self.fake.latitude()),
                'LONGITUDE': float(self.fake.longitude())
            })
        
        return pd.DataFrame(data)
    
    def generate_customers(self):
        """Generate customers"""
        segments = list(range(1, 7))  # 6 segments
        customer_types = ['INDIVIDUAL', 'BUSINESS']
        educations = ['High School', 'Bachelor', 'Master', 'PhD', 'Associate']
        
        data = []
        
        for i in range(1, self.num_customers + 1):
            customer_type = random.choice(customer_types, weights=[0.7, 0.3])
            is_business = customer_type == 'BUSINESS'
            
            # Generate realistic annual income based on customer type
            if is_business:
                annual_income = random.uniform(50000, 10000000)
            else:
                annual_income = random.uniform(20000, 200000)
            
            registration_date = self.fake.date_between(start_date=self.start_date, end_date=self.end_date)
            
            data.append({
                'CUSTOMER_ID': i,
                'CUSTOMER_NUMBER': f'C{i:08d}',
                'CUSTOMER_TYPE': customer_type,
                'COMPANY_NAME': self.fake.company() if is_business else None,
                'FIRST_NAME': None if is_business else self.fake.first_name(),
                'LAST_NAME': None if is_business else self.fake.last_name(),
                'TITLE': self.fake.prefix() if not is_business else None,
                'EMAIL': self.fake.email(),
                'PHONE': self.fake.phone_number(),
                'MOBILE': self.fake.phone_number() if random.random() < 0.7 else None,
                'BIRTH_DATE': self.fake.date_of_birth(minimum_age=18, maximum_age=80) if not is_business else None,
                'GENDER': random.choice(['M', 'F', 'Other']) if not is_business else None,
                'MARITAL_STATUS': random.choice(['Single', 'Married', 'Divorced', 'Widowed']) if not is_business else None,
                'EDUCATION': random.choice(educations) if not is_business else None,
                'OCCUPATION': self.fake.job() if not is_business else None,
                'ANNUAL_INCOME': round(annual_income, 2),
                'SEGMENT_ID': random.choice(segments),
                'BILLING_ADDRESS_ID': random.randint(1, 75000),
                'SHIPPING_ADDRESS_ID': random.randint(1, 75000),
                'REGISTRATION_DATE': registration_date,
                'LAST_ORDER_DATE': None,  # Will be updated when orders are generated
                'TOTAL_ORDERS': 0,
                'TOTAL_SPENT': 0.00,
                'CREDIT_LIMIT': round(annual_income * random.uniform(0.1, 0.5), 2),
                'STATUS': random.choice(['ACTIVE', 'INACTIVE'], weights=[0.95, 0.05]),
                'PREFERRED_LANGUAGE': random.choice(['EN', 'ES', 'FR'], weights=[0.8, 0.15, 0.05]),
                'MARKETING_OPT_IN': random.choice([True, False], weights=[0.7, 0.3])
            })
        
        return pd.DataFrame(data)
    
    def generate_departments(self):
        """Generate departments"""
        departments = [
            {'name': 'Sales', 'code': 'SALES', 'budget': 2000000},
            {'name': 'Marketing', 'code': 'MKT', 'budget': 1500000},
            {'name': 'Information Technology', 'code': 'IT', 'budget': 3000000},
            {'name': 'Human Resources', 'code': 'HR', 'budget': 800000},
            {'name': 'Finance', 'code': 'FIN', 'budget': 1200000},
            {'name': 'Operations', 'code': 'OPS', 'budget': 2500000},
            {'name': 'Customer Service', 'code': 'CS', 'budget': 1000000},
            {'name': 'Research & Development', 'code': 'RD', 'budget': 4000000},
            {'name': 'Legal', 'code': 'LEG', 'budget': 600000},
            {'name': 'Procurement', 'code': 'PROC', 'budget': 500000}
        ]
        
        data = []
        for i, dept in enumerate(departments, 1):
            data.append({
                'DEPARTMENT_ID': i,
                'DEPARTMENT_NAME': dept['name'],
                'DEPARTMENT_CODE': dept['code'],
                'DESCRIPTION': f'{dept["name"]} department',
                'BUDGET': dept['budget'],
                'LOCATION': self.fake.city(),
                'PHONE': self.fake.phone_number(),
                'EMAIL': f'{dept["code"].lower()}@retailworks.com'
            })
        
        return pd.DataFrame(data)
    
    def generate_positions(self):
        """Generate job positions"""
        positions = [
            {'title': 'CEO', 'code': 'CEO', 'dept_id': 1, 'level': 10, 'min_sal': 300000, 'max_sal': 500000},
            {'title': 'VP Sales', 'code': 'VP_SALES', 'dept_id': 1, 'level': 9, 'min_sal': 200000, 'max_sal': 300000},
            {'title': 'Sales Manager', 'code': 'SALES_MGR', 'dept_id': 1, 'level': 7, 'min_sal': 80000, 'max_sal': 120000},
            {'title': 'Sales Representative', 'code': 'SALES_REP', 'dept_id': 1, 'level': 5, 'min_sal': 45000, 'max_sal': 75000},
            {'title': 'Marketing Director', 'code': 'MKT_DIR', 'dept_id': 2, 'level': 8, 'min_sal': 120000, 'max_sal': 180000},
            {'title': 'Marketing Manager', 'code': 'MKT_MGR', 'dept_id': 2, 'level': 6, 'min_sal': 65000, 'max_sal': 95000},
            {'title': 'CTO', 'code': 'CTO', 'dept_id': 3, 'level': 10, 'min_sal': 250000, 'max_sal': 400000},
            {'title': 'IT Manager', 'code': 'IT_MGR', 'dept_id': 3, 'level': 7, 'min_sal': 90000, 'max_sal': 130000},
            {'title': 'Software Engineer', 'code': 'SW_ENG', 'dept_id': 3, 'level': 5, 'min_sal': 70000, 'max_sal': 120000},
            {'title': 'Data Analyst', 'code': 'DATA_ANAL', 'dept_id': 3, 'level': 4, 'min_sal': 55000, 'max_sal': 85000},
            {'title': 'HR Director', 'code': 'HR_DIR', 'dept_id': 4, 'level': 8, 'min_sal': 100000, 'max_sal': 150000},
            {'title': 'HR Manager', 'code': 'HR_MGR', 'dept_id': 4, 'level': 6, 'min_sal': 60000, 'max_sal': 90000},
            {'title': 'CFO', 'code': 'CFO', 'dept_id': 5, 'level': 10, 'min_sal': 200000, 'max_sal': 350000},
            {'title': 'Finance Manager', 'code': 'FIN_MGR', 'dept_id': 5, 'level': 7, 'min_sal': 75000, 'max_sal': 110000},
            {'title': 'Accountant', 'code': 'ACCT', 'dept_id': 5, 'level': 4, 'min_sal': 45000, 'max_sal': 70000}
        ]
        
        data = []
        for i, pos in enumerate(positions, 1):
            data.append({
                'POSITION_ID': i,
                'POSITION_TITLE': pos['title'],
                'POSITION_CODE': pos['code'],
                'DEPARTMENT_ID': pos['dept_id'],
                'JOB_LEVEL': pos['level'],
                'MIN_SALARY': pos['min_sal'],
                'MAX_SALARY': pos['max_sal'],
                'DESCRIPTION': f'{pos["title"]} position in {pos["code"]} department',
                'STATUS': 'ACTIVE'
            })
        
        return pd.DataFrame(data)
    
    def save_dataframes_to_csv(self):
        """Save all generated dataframes to CSV files"""
        output_dir = '/Users/saurabhrai/Documents/CursorWorkSpace/SnowflakeLearner/retailworks-snowflake/dml/sample_data/'
        
        # Generate all data
        print("Generating customer segments...")
        customer_segments_df = self.generate_customer_segments()
        customer_segments_df.to_csv(f'{output_dir}customer_segments.csv', index=False)
        
        print("Generating categories...")
        categories_df = self.generate_categories()
        categories_df.to_csv(f'{output_dir}categories.csv', index=False)
        
        print("Generating suppliers...")
        suppliers_df = self.generate_suppliers()
        suppliers_df.to_csv(f'{output_dir}suppliers.csv', index=False)
        
        print("Generating products...")
        products_df = self.generate_products()
        products_df.to_csv(f'{output_dir}products.csv', index=False)
        
        print("Generating addresses...")
        addresses_df = self.generate_addresses()
        addresses_df.to_csv(f'{output_dir}addresses.csv', index=False)
        
        print("Generating customers...")
        customers_df = self.generate_customers()
        customers_df.to_csv(f'{output_dir}customers.csv', index=False)
        
        print("Generating departments...")
        departments_df = self.generate_departments()
        departments_df.to_csv(f'{output_dir}departments.csv', index=False)
        
        print("Generating positions...")
        positions_df = self.generate_positions()
        positions_df.to_csv(f'{output_dir}positions.csv', index=False)
        
        print("Sample data generation completed!")
        print(f"Generated files:")
        print(f"- customer_segments.csv: {len(customer_segments_df)} records")
        print(f"- categories.csv: {len(categories_df)} records")
        print(f"- suppliers.csv: {len(suppliers_df)} records")
        print(f"- products.csv: {len(products_df)} records")
        print(f"- addresses.csv: {len(addresses_df)} records")
        print(f"- customers.csv: {len(customers_df)} records")
        print(f"- departments.csv: {len(departments_df)} records")
        print(f"- positions.csv: {len(positions_df)} records")

if __name__ == "__main__":
    generator = RetailWorksDataGenerator()
    generator.save_dataframes_to_csv()