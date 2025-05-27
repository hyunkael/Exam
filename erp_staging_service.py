import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Column, String, Integer, Float, Date, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection
DB_URL = "postgresql://production_unmx_user:WDJB0GjyyQDXkPH7MATl2ITXK2z0EUuY@dpg-d0q2h9re5dus73eb7ftg-a.singapore-postgres.render.com/production_unmx"
engine = create_engine(DB_URL)
Base = declarative_base()
metadata = MetaData()

# Define the source directory - try multiple possible locations
def find_source_dir():
    possible_paths = [
        "data_engineering_practice-main/source_erp",  # Original path
        "source_erp",                              # Direct path
        "../data_engineering_practice-main/source_erp",  # One level up
        "./source_erp"                             # Current directory
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            logger.info(f"Found source directory at: {path}")
            return path
    
    # If we get here, we couldn't find the directory
    logger.error("Could not find source_erp directory. Using default path.")
    return "source_erp"  # Default to this and let it fail with clear error if needed

SOURCE_DIR = find_source_dir()

def create_tables():
    """Create tables for each CSV file in the ERP folder"""
    try:
        # Customer Demographics table (CUST_AZ12.csv)
        customer_demographics = Table(
            'erp_customer_demographics', 
            metadata,
            Column('CID', String(255), primary_key=True),
            Column('BDATE', Date),
            Column('GEN', String(10))
        )
        
        # Customer Location table (LOC_A101.csv)
        customer_location = Table(
            'erp_customer_location', 
            metadata,
            Column('CID', String(255), primary_key=True),
            Column('CNTRY', String(255))
        )
        
        # Product Categories table (PX_CAT_G1V2.csv)
        product_categories = Table(
            'erp_product_categories', 
            metadata,
            Column('ID', String(255), primary_key=True),
            Column('CAT', String(255)),
            Column('SUBCAT', String(255)),
            Column('MAINTENANCE', String(10))
        )
        
        metadata.create_all(engine)
        logger.info("ERP tables created successfully")
        
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise

def load_data():
    """Load data from CSV files into the database tables"""
    try:
        # Check if we can access the files
        files_to_check = [
            f"{SOURCE_DIR}/CUST_AZ12.csv",
            f"{SOURCE_DIR}/LOC_A101.csv",
            f"{SOURCE_DIR}/PX_CAT_G1V2.csv"
        ]
        
        for file_path in files_to_check:
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                logger.error(f"Current working directory: {os.getcwd()}")
                logger.error(f"Directory contents: {os.listdir(os.path.dirname(file_path) if os.path.dirname(file_path) else '.')}")
                raise FileNotFoundError(f"Could not find {file_path}")
        
        # Load customer demographics
        logger.info(f"Loading customer demographics from {SOURCE_DIR}/CUST_AZ12.csv")
        customer_demo_df = pd.read_csv(f"{SOURCE_DIR}/CUST_AZ12.csv")
        customer_demo_df['BDATE'] = pd.to_datetime(customer_demo_df['BDATE'])
        customer_demo_df.to_sql('erp_customer_demographics', engine, if_exists='replace', index=False)
        logger.info(f"Loaded {len(customer_demo_df)} records into erp_customer_demographics")
        
        # Load customer location
        logger.info(f"Loading customer location from {SOURCE_DIR}/LOC_A101.csv")
        customer_loc_df = pd.read_csv(f"{SOURCE_DIR}/LOC_A101.csv")
        # Clean up CID by removing hyphens to match with other tables
        customer_loc_df['CID'] = customer_loc_df['CID'].str.replace('-', '')
        customer_loc_df.to_sql('erp_customer_location', engine, if_exists='replace', index=False)
        logger.info(f"Loaded {len(customer_loc_df)} records into erp_customer_location")
        
        # Load product categories
        logger.info(f"Loading product categories from {SOURCE_DIR}/PX_CAT_G1V2.csv")
        product_cat_df = pd.read_csv(f"{SOURCE_DIR}/PX_CAT_G1V2.csv")
        product_cat_df.to_sql('erp_product_categories', engine, if_exists='replace', index=False)
        logger.info(f"Loaded {len(product_cat_df)} records into erp_product_categories")
        
    except FileNotFoundError as e:
        logger.error(f"File not found error: {e}")
        logger.error("Please ensure the CSV files are available in the correct location")
        raise
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise



def clean_data():
    """Clean and transform the data"""
    try:
        # Standardize gender values
        with engine.connect() as conn:
            conn.execute("UPDATE erp_customer_demographics SET GEN = 'M' WHERE GEN = 'Male'")
            conn.execute("UPDATE erp_customer_demographics SET GEN = 'F' WHERE GEN = 'Female'")
            logger.info("Standardized gender values in customer demographics")
            
        # Ensure maintenance values are standardized to boolean
        with engine.connect() as conn:
            conn.execute("UPDATE erp_product_categories SET MAINTENANCE = 'true' WHERE MAINTENANCE = 'Yes'")
            conn.execute("UPDATE erp_product_categories SET MAINTENANCE = 'false' WHERE MAINTENANCE = 'No'")
            logger.info("Standardized maintenance values in product categories")
            
    except Exception as e:
        logger.error(f"Error cleaning data: {e}")
        raise

def main():
    """Main function to run the ERP staging service"""
    logger.info("Starting ERP staging service")
    create_tables()
    load_data()
    clean_data()
    logger.info("ERP staging service completed successfully")

if __name__ == "__main__":
    main()
