import os
import pandas as pd
from sqlalchemy import create_engine, Column, String, Integer, Float, Date, MetaData, Table, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connections
CRM_DB_URL = "postgresql://development_va0t_user:7DPz2GrySQ2e2GdtCrPwd1WiUQmJMRLR@dpg-d0q2gvre5dus73eb7600-a.singapore-postgres.render.com/development_va0t"
ERP_DB_URL = "postgresql://production_unmx_user:WDJB0GjyyQDXkPH7MATl2ITXK2z0EUuY@dpg-d0q2h9re5dus73eb7ftg-a.singapore-postgres.render.com/production_unmx"
DW_DB_URL = "postgresql://warehouse_0uj2_user:ZD55uP1cyY7p7BWx7KeMek1az3GzL3SI@dpg-d0qmhfidbo4c73c9d2e0-a.singapore-postgres.render.com/warehouse_0uj2"

crm_engine = create_engine(CRM_DB_URL)
erp_engine = create_engine(ERP_DB_URL)
dw_engine = create_engine(DW_DB_URL)

Base = declarative_base()
metadata = MetaData()

def create_warehouse_tables():
    """Create integrated tables in the data warehouse"""
    try:
        # Dimension table for customers
        dim_customers = Table(
            'dim_customers', 
            metadata,
            Column('customer_id', Integer, primary_key=True),
            Column('customer_key', String(255)),
            Column('first_name', String(255)),
            Column('last_name', String(255)),
            Column('gender', String(10)),
            Column('marital_status', String(10)),
            Column('birth_date', Date),
            Column('country', String(255)),
            Column('create_date', Date)
        )
        
        # Dimension table for products
        dim_products = Table(
            'dim_products', 
            metadata,
            Column('product_id', Integer, primary_key=True),
            Column('product_key', String(255)),
            Column('product_name', String(255)),
            Column('product_cost', Float),
            Column('product_line', String(10)),
            Column('category', String(255)),
            Column('subcategory', String(255)),
            Column('maintenance_required', String(10)),
            Column('start_date', Date),
            Column('end_date', Date)
        )
        
        # Fact table for sales
        fact_sales = Table(
            'fact_sales', 
            metadata,
            Column('order_number', String(255), primary_key=True),
            Column('product_key', String(255)),
            Column('customer_id', Integer),
            Column('order_date', Date),
            Column('ship_date', Date),
            Column('due_date', Date),
            Column('sales_amount', Float),
            Column('quantity', Integer),
            Column('unit_price', Float)
        )
        
        metadata.create_all(dw_engine)
        logger.info("Data warehouse tables created successfully")
        
    except Exception as e:
        logger.error(f"Error creating data warehouse tables: {e}")
        raise

def extract_and_transform_data():
    """Extract data from staging areas and transform it for the data warehouse"""
    try:
        # Extract customer data from CRM
        crm_customers = pd.read_sql("SELECT * FROM crm_customer_info", crm_engine)
        
        # Extract customer data from ERP
        erp_demographics = pd.read_sql("SELECT * FROM erp_customer_demographics", erp_engine)
        erp_locations = pd.read_sql("SELECT * FROM erp_customer_location", erp_engine)
        
        # Extract product data
        crm_products = pd.read_sql("SELECT * FROM crm_product_info", crm_engine)
        erp_categories = pd.read_sql("SELECT * FROM erp_product_categories", erp_engine)
        
        # Extract sales data
        crm_sales = pd.read_sql("SELECT * FROM crm_sales_details", crm_engine)
        
        # Transform customer data
        # Prepare customer dimension by joining CRM and ERP data
        # First, extract the customer ID from the ERP CID (remove the 'NASA' prefix)
        erp_demographics['erp_customer_key'] = erp_demographics['CID'].str.replace('NASA', '')
        
        # Merge with CRM customer data
        dim_customers_df = pd.merge(
            crm_customers,
            erp_demographics,
            left_on='cst_key',
            right_on='erp_customer_key',
            how='left'
        )
        
        # Merge with location data
        erp_locations['location_customer_key'] = erp_locations['CID'].str.replace('-', '')
        dim_customers_df = pd.merge(
            dim_customers_df,
            erp_locations,
            left_on='cst_key',
            right_on='location_customer_key',
            how='left'
        )
        
        # Rename and select columns for the final dimension table
        dim_customers_df = dim_customers_df.rename(columns={
            'cst_id': 'customer_id',
            'cst_key': 'customer_key',
            'cst_firstname': 'first_name',
            'cst_lastname': 'last_name',
            'cst_gndr': 'gender',
            'cst_marital_status': 'marital_status',
            'BDATE': 'birth_date',
            'CNTRY': 'country',
            'cst_create_date': 'create_date'
        })
        
        dim_customers_df = dim_customers_df[[
            'customer_id', 'customer_key', 'first_name', 'last_name', 
            'gender', 'marital_status', 'birth_date', 'country', 'create_date'
        ]]
        
        # Transform product data
        # Extract category prefix from product key to join with category data
        crm_products['category_prefix'] = crm_products['prd_key'].str.split('-').str[0]
        
        # Merge with category data
        dim_products_df = pd.merge(
            crm_products,
            erp_categories,
            left_on='category_prefix',
            right_on='ID',
            how='left'
        )
        
        # Rename and select columns for the final dimension table
        dim_products_df = dim_products_df.rename(columns={
            'prd_id': 'product_id',
            'prd_key': 'product_key',
            'prd_nm': 'product_name',
            'prd_cost': 'product_cost',
            'prd_line': 'product_line',
            'CAT': 'category',
            'SUBCAT': 'subcategory',
            'MAINTENANCE': 'maintenance_required',
            'prd_start_dt': 'start_date',
            'prd_end_dt': 'end_date'
        })
        
        dim_products_df = dim_products_df[[
            'product_id', 'product_key', 'product_name', 'product_cost', 
            'product_line', 'category', 'subcategory', 'maintenance_required',
            'start_date', 'end_date'
        ]]
        
        # Transform sales data
        fact_sales_df = crm_sales.rename(columns={
            'sls_ord_num': 'order_number',
            'sls_prd_key': 'product_key',
            'sls_cust_id': 'customer_id',
            'sls_order_dt': 'order_date',
            'sls_ship_dt': 'ship_date',
            'sls_due_dt': 'due_date',
            'sls_sales': 'sales_amount',
            'sls_quantity': 'quantity',
            'sls_price': 'unit_price'
        })
        
        return dim_customers_df, dim_products_df, fact_sales_df
        
    except Exception as e:
        logger.error(f"Error extracting and transforming data: {e}")
        raise

def load_to_warehouse(dim_customers_df, dim_products_df, fact_sales_df):
    """Load the transformed data into the data warehouse"""
    try:
        # Load dimension tables
        dim_customers_df.to_sql('dim_customers', dw_engine, if_exists='replace', index=False)
        logger.info(f"Loaded {len(dim_customers_df)} records into dim_customers")
        
        dim_products_df.to_sql('dim_products', dw_engine, if_exists='replace', index=False)
        logger.info(f"Loaded {len(dim_products_df)} records into dim_products")
        
        # Load fact table
        fact_sales_df.to_sql('fact_sales', dw_engine, if_exists='replace', index=False)
        logger.info(f"Loaded {len(fact_sales_df)} records into fact_sales")
        
    except Exception as e:
        logger.error(f"Error loading data to warehouse: {e}")
        raise

def create_views():
    """Create useful views for analytics"""
    try:
        with dw_engine.connect() as conn:
            # Sales by customer view
            conn.execute(text("""
                CREATE OR REPLACE VIEW vw_sales_by_customer AS
                SELECT 
                    c.customer_id,
                    c.first_name || ' ' || c.last_name AS customer_name,
                    c.country,
                    COUNT(s.order_number) AS order_count,
                    SUM(s.sales_amount) AS total_sales
                FROM 
                    fact_sales s
                JOIN 
                    dim_customers c ON s.customer_id = c.customer_id
                GROUP BY 
                    c.customer_id, customer_name, c.country
                ORDER BY 
                    total_sales DESC
            """))
            
            # Sales by product view
            conn.execute(text("""
                CREATE OR REPLACE VIEW vw_sales_by_product AS
                SELECT 
                    p.product_id,
                    p.product_name,
                    p.category,
                    p.subcategory,
                    COUNT(s.order_number) AS order_count,
                    SUM(s.quantity) AS total_quantity,
                    SUM(s.sales_amount) AS total_sales
                FROM 
                    fact_sales s
                JOIN 
                    dim_products p ON s.product_key = p.product_key
                GROUP BY 
                    p.product_id, p.product_name, p.category, p.subcategory
                ORDER BY 
                    total_sales DESC
            """))
            
            # Sales by date view
            conn.execute(text("""
                CREATE OR REPLACE VIEW vw_sales_by_date AS
                SELECT 
                    EXTRACT(YEAR FROM order_date) AS year,
                    EXTRACT(MONTH FROM order_date) AS month,
                    COUNT(order_number) AS order_count,
                    SUM(sales_amount) AS total_sales
                FROM 
                    fact_sales
                GROUP BY 
                    year, month
                ORDER BY 
                    year, month
            """))
            
            logger.info("Created analytics views successfully")
            
    except Exception as e:
        logger.error(f"Error creating views: {e}")
        raise

def main():
    """Main function to run the data warehouse service"""
    logger.info("Starting data warehouse service")
    
    create_warehouse_tables()
    dim_customers_df, dim_products_df, fact_sales_df = extract_and_transform_data()
    load_to_warehouse(dim_customers_df, dim_products_df, fact_sales_df)
    create_views()
    
    logger.info("Data warehouse service completed successfully")

if __name__ == "__main__":
    main()
