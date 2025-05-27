import os
import pandas as pd
from sqlalchemy import create_engine, Column, String, Integer, Float, Date, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection
DB_URL = "postgresql://development_va0t_user:7DPz2GrySQ2e2GdtCrPwd1WiUQmJMRLR@dpg-d0q2gvre5dus73eb7600-a.singapore-postgres.render.com/development_va0t"
engine = create_engine(DB_URL)
Base = declarative_base()
metadata = MetaData()

# Define the source directory
SOURCE_DIR = "data_engineering_practice-main/source_crm"

def create_tables():
    """Create tables for each CSV file in the CRM folder"""
    try:
        # Customer Info table
        customer_info = Table(
            'crm_customer_info', 
            metadata,
            Column('cst_id', Integer, primary_key=True),
            Column('cst_key', String(255)),
            Column('cst_firstname', String(255)),
            Column('cst_lastname', String(255)),
            Column('cst_marital_status', String(10)),
            Column('cst_gndr', String(10)),
            Column('cst_create_date', Date)
        )
        
        # Product Info table
        product_info = Table(
            'crm_product_info', 
            metadata,
            Column('prd_id', Integer, primary_key=True),
            Column('prd_key', String(255)),
            Column('prd_nm', String(255)),
            Column('prd_cost', Float),
            Column('prd_line', String(10)),
            Column('prd_start_dt', Date),
            Column('prd_end_dt', Date)
        )
        
        # Sales Details table
        sales_details = Table(
            'crm_sales_details', 
            metadata,
            Column('sls_ord_num', String(255), primary_key=True),
            Column('sls_prd_key', String(255)),
            Column('sls_cust_id', Integer),
            Column('sls_order_dt', Date),
            Column('sls_ship_dt', Date),
            Column('sls_due_dt', Date),
            Column('sls_sales', Float),
            Column('sls_quantity', Integer),
            Column('sls_price', Float)
        )
        
        metadata.create_all(engine)
        logger.info("CRM tables created successfully")
        
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise

def load_data():
    """Load data from CSV files into the database tables"""
    try:
        # Load customer info
        customer_df = pd.read_csv(f"{SOURCE_DIR}/cust_info.csv")
        customer_df['cst_create_date'] = pd.to_datetime(customer_df['cst_create_date'])
        customer_df.to_sql('crm_customer_info', engine, if_exists='replace', index=False)
        logger.info(f"Loaded {len(customer_df)} records into crm_customer_info")
        
        # Load product info
        product_df = pd.read_csv(f"{SOURCE_DIR}/prd_info.csv")
        product_df['prd_start_dt'] = pd.to_datetime(product_df['prd_start_dt'])
        product_df['prd_end_dt'] = pd.to_datetime(product_df['prd_end_dt'])
        product_df.to_sql('crm_product_info', engine, if_exists='replace', index=False)
        logger.info(f"Loaded {len(product_df)} records into crm_product_info")
        
        # Load sales details
        sales_df = pd.read_csv(f"{SOURCE_DIR}/sales_details.csv")
        # Convert date columns from numeric format to datetime
        sales_df['sls_order_dt'] = pd.to_datetime(sales_df['sls_order_dt'], format='%Y%m%d')
        sales_df['sls_ship_dt'] = pd.to_datetime(sales_df['sls_ship_dt'], format='%Y%m%d')
        sales_df['sls_due_dt'] = pd.to_datetime(sales_df['sls_due_dt'], format='%Y%m%d')
        sales_df.to_sql('crm_sales_details', engine, if_exists='replace', index=False)
        logger.info(f"Loaded {len(sales_df)} records into crm_sales_details")
        
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def clean_data():
    """Clean and transform the data"""
    try:
        # Clean customer info - trim whitespace
        with engine.connect() as conn:
            conn.execute("UPDATE crm_customer_info SET cst_firstname = TRIM(cst_firstname), cst_lastname = TRIM(cst_lastname)")
            logger.info("Cleaned customer info data")
            
        # Handle missing values in product info
        with engine.connect() as conn:
            conn.execute("UPDATE crm_product_info SET prd_cost = 0 WHERE prd_cost IS NULL")
            logger.info("Cleaned product info data")
            
    except Exception as e:
        logger.error(f"Error cleaning data: {e}")
        raise

def main():
    """Main function to run the CRM staging service"""
    logger.info("Starting CRM staging service")
    create_tables()
    load_data()
    clean_data()
    logger.info("CRM staging service completed successfully")

if __name__ == "__main__":
    main()
