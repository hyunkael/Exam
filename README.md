# Data Engineering Pipeline on Render

This project implements a complete data engineering pipeline deployed on Render, consisting of:

1. Two staging area services for CRM and ERP data
2. One data warehouse service that combines cleaned data
3. A Streamlit dashboard for data visualization

## Project Structure

- `crm_staging_service.py` - Processes CRM data files and loads them into the staging database
- `erp_staging_service.py` - Processes ERP data files and loads them into the staging database
- `data_warehouse_service.py` - Extracts data from staging areas, transforms it, and loads it into the data warehouse
- `dashboard.py` - Streamlit dashboard to visualize the integrated data
- `Procfile` - Configuration file for Render deployment
- `requirements.txt` - Python dependencies

## Data Sources

### CRM Data
- `cust_info.csv` - Customer information including ID, name, gender, and marital status
- `prd_info.csv` - Product information including ID, name, cost, and dates
- `sales_details.csv` - Sales transaction data

### ERP Data
- `CUST_AZ12.csv` - Customer demographic information including birth date and gender
- `LOC_A101.csv` - Customer location information
- `PX_CAT_G1V2.csv` - Product category information

## Database Structure

### Staging Databases

#### CRM Staging (Database 1)
- `crm_customer_info`
- `crm_product_info`
- `crm_sales_details`

#### ERP Staging (Database 2)
- `erp_customer_demographics`
- `erp_customer_location`
- `erp_product_categories`

### Data Warehouse (Database 2)
- Dimension Tables:
  - `dim_customers` - Integrated customer information
  - `dim_products` - Integrated product information
- Fact Table:
  - `fact_sales` - Sales transactions
- Views:
  - `vw_sales_by_customer` - Sales aggregated by customer
  - `vw_sales_by_product` - Sales aggregated by product
  - `vw_sales_by_date` - Sales aggregated by date

## Deployment Instructions

1. Create two PostgreSQL databases on Render
2. Update the database connection strings in the Python files if needed
3. Deploy the services to Render using the Procfile
4. The services should be deployed in the following order:
   - CRM Staging Service
   - ERP Staging Service
   - Data Warehouse Service
   - Dashboard

## Dashboard Features

The Streamlit dashboard provides:
- Key performance indicators
- Sales trends over time
- Top customers by sales
- Top products by sales
- Customer demographic analysis
- Product category analysis

## Dependencies

- pandas
- psycopg2-binary
- sqlalchemy
- streamlit
- plotly
- python-dotenv
