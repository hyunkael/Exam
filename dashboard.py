import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection
DW_DB_URL = "postgresql://warehouse_0uj2_user:ZD55uP1cyY7p7BWx7KeMek1az3GzL3SI@dpg-d0qmhfidbo4c73c9d2e0-a.singapore-postgres.render.com/warehouse_0uj2"
engine = create_engine(DW_DB_URL)

# Set page config
st.set_page_config(
    page_title="Sales Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E88E5;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #333;
        margin-top: 1rem;
    }
    .card {
        background-color: #f9f9f9;
        border-radius: 10px;
        padding: 1.5rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
</style>
""", unsafe_allow_html=True)

def create_views_if_not_exist():
    """Create the necessary views if they don't exist"""
    try:
        with engine.connect() as conn:
            # Check if tables exist first
            tables_exist = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'dim_customers'
                ) AND EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'dim_products'
                ) AND EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'fact_sales'
                );
            """)).scalar()
            
            if not tables_exist:
                logger.warning("Required tables don't exist yet. Data warehouse service needs to run first.")
                return False
            
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
                    c.customer_id, c.first_name, c.last_name, c.country
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
            return True
            
    except Exception as e:
        logger.error(f"Error creating views: {e}")
        return False

def load_data():
    """Load data from the data warehouse"""
    try:
        # Try to create views if they don't exist
        views_created = create_views_if_not_exist()
        
        # If views couldn't be created, use direct queries instead
        if views_created:
            # Load from views
            sales_by_customer = pd.read_sql("SELECT * FROM vw_sales_by_customer", engine)
            sales_by_product = pd.read_sql("SELECT * FROM vw_sales_by_product", engine)
            sales_by_date = pd.read_sql("SELECT * FROM vw_sales_by_date", engine)
        else:
            # Use direct queries instead of views
            sales_by_customer = pd.read_sql(text("""
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
                    c.customer_id, c.first_name, c.last_name, c.country
                ORDER BY 
                    total_sales DESC
            """), engine)
            
            sales_by_product = pd.read_sql(text("""
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
            """), engine)
            
            sales_by_date = pd.read_sql(text("""
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
            """), engine)
        
        # Load raw data for detailed analysis
        try:
            customers = pd.read_sql("SELECT * FROM dim_customers", engine)
            products = pd.read_sql("SELECT * FROM dim_products", engine)
            sales = pd.read_sql("SELECT * FROM fact_sales", engine)
            
            return {
                'sales_by_customer': sales_by_customer,
                'sales_by_product': sales_by_product,
                'sales_by_date': sales_by_date,
                'customers': customers,
                'products': products,
                'sales': sales
            }
        except Exception as e:
            logger.error(f"Error loading raw data: {e}")
            st.warning("The data warehouse tables don't exist yet. Please run the data warehouse service first.")
            return None
            
    except Exception as e:
        st.error(f"Error loading data: {e}")
        logger.error(f"Error loading data: {e}")
        return None

def create_kpi_metrics(data):
    """Create KPI metrics for the dashboard"""
    sales_df = data['sales']
    customers_df = data['customers']
    products_df = data['products']
    
    total_sales = sales_df['sales_amount'].sum()
    total_orders = len(sales_df)
    total_customers = len(customers_df)
    total_products = len(products_df)
    avg_order_value = total_sales / total_orders if total_orders > 0 else 0
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Sales", f"${total_sales:,.2f}")
    
    with col2:
        st.metric("Total Orders", f"{total_orders:,}")
    
    with col3:
        st.metric("Total Customers", f"{total_customers:,}")
    
    with col4:
        st.metric("Total Products", f"{total_products:,}")
    
    with col5:
        st.metric("Avg Order Value", f"${avg_order_value:,.2f}")

def create_sales_by_customer_chart(data):
    """Create sales by customer chart"""
    df = data['sales_by_customer'].sort_values('total_sales', ascending=False).head(10)
    
    fig = px.bar(
        df,
        x='customer_name',
        y='total_sales',
        color='total_sales',
        color_continuous_scale='Blues',
        title='Top 10 Customers by Sales',
        labels={'customer_name': 'Customer', 'total_sales': 'Total Sales ($)'}
    )
    
    fig.update_layout(
        xaxis_tickangle=-45,
        height=500,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    st.plotly_chart(fig, use_container_width=True)

def create_sales_by_product_chart(data):
    """Create sales by product chart"""
    df = data['sales_by_product'].sort_values('total_sales', ascending=False).head(10)
    
    fig = px.bar(
        df,
        x='product_name',
        y='total_sales',
        color='category',
        title='Top 10 Products by Sales',
        labels={'product_name': 'Product', 'total_sales': 'Total Sales ($)', 'category': 'Category'}
    )
    
    fig.update_layout(
        xaxis_tickangle=-45,
        height=500,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    st.plotly_chart(fig, use_container_width=True)

def create_sales_trend_chart(data):
    """Create sales trend chart"""
    df = data['sales_by_date']
    df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str) + '-01')
    df = df.sort_values('date')
    
    fig = px.line(
        df,
        x='date',
        y='total_sales',
        markers=True,
        title='Sales Trend by Month',
        labels={'date': 'Month', 'total_sales': 'Total Sales ($)'}
    )
    
    fig.update_layout(
        height=500,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    st.plotly_chart(fig, use_container_width=True)

def create_sales_by_category_chart(data):
    """Create sales by category chart"""
    df = data['sales_by_product']
    category_sales = df.groupby('category')['total_sales'].sum().reset_index()
    
    fig = px.pie(
        category_sales,
        values='total_sales',
        names='category',
        title='Sales by Product Category',
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    
    fig.update_layout(
        height=500,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    st.plotly_chart(fig, use_container_width=True)

def create_customer_demographics_chart(data):
    """Create customer demographics chart"""
    df = data['customers']
    
    # Gender distribution
    gender_counts = df['gender'].value_counts().reset_index()
    gender_counts.columns = ['Gender', 'Count']
    
    # Marital status distribution
    marital_counts = df['marital_status'].value_counts().reset_index()
    marital_counts.columns = ['Marital Status', 'Count']
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig1 = px.pie(
            gender_counts,
            values='Count',
            names='Gender',
            title='Customer Gender Distribution',
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig1.update_layout(height=400)
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        fig2 = px.pie(
            marital_counts,
            values='Count',
            names='Marital Status',
            title='Customer Marital Status Distribution',
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)

def create_country_distribution_chart(data):
    """Create country distribution chart"""
    df = data['customers']
    country_counts = df['country'].value_counts().reset_index()
    country_counts.columns = ['Country', 'Count']
    
    fig = px.bar(
        country_counts,
        x='Country',
        y='Count',
        color='Count',
        color_continuous_scale='Viridis',
        title='Customer Distribution by Country',
        labels={'Country': 'Country', 'Count': 'Number of Customers'}
    )
    
    fig.update_layout(
        xaxis_tickangle=0,
        height=500,
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    st.plotly_chart(fig, use_container_width=True)

def main():
    """Main function to run the Streamlit dashboard"""
    st.markdown('<div class="main-header">Sales Data Dashboard</div>', unsafe_allow_html=True)
    st.markdown('This dashboard visualizes the integrated data from CRM and ERP systems.')
    
    # Load data
    data = load_data()
    
    if data:
        # Create tabs for different dashboard sections
        tab1, tab2, tab3 = st.tabs(["Sales Overview", "Customer Analysis", "Product Analysis"])
        
        with tab1:
            st.markdown('<div class="sub-header">Key Performance Indicators</div>', unsafe_allow_html=True)
            create_kpi_metrics(data)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown('<div class="card">', unsafe_allow_html=True)
                create_sales_trend_chart(data)
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown('<div class="card">', unsafe_allow_html=True)
                create_sales_by_category_chart(data)
                st.markdown('</div>', unsafe_allow_html=True)
        
        with tab2:
            st.markdown('<div class="sub-header">Customer Analysis</div>', unsafe_allow_html=True)
            
            create_sales_by_customer_chart(data)
            
            st.markdown('<div class="sub-header">Customer Demographics</div>', unsafe_allow_html=True)
            create_customer_demographics_chart(data)
            
            st.markdown('<div class="sub-header">Customer Geographic Distribution</div>', unsafe_allow_html=True)
            create_country_distribution_chart(data)
            
            # Show raw customer data
            with st.expander("View Raw Customer Data"):
                st.dataframe(data['customers'])
        
        with tab3:
            st.markdown('<div class="sub-header">Product Analysis</div>', unsafe_allow_html=True)
            
            create_sales_by_product_chart(data)
            
            # Product categories breakdown
            product_categories = data['products'].groupby(['category', 'subcategory']).size().reset_index(name='count')
            
            st.markdown('<div class="sub-header">Product Categories</div>', unsafe_allow_html=True)
            st.dataframe(product_categories)
            
            # Show raw product data
            with st.expander("View Raw Product Data"):
                st.dataframe(data['products'])
    
    else:
        st.warning("Failed to load data from the data warehouse.")
        st.info("""
        ### Deployment Order
        Please ensure you've deployed the services in the correct order:
        
        1. First run the CRM Staging Service: `python crm_staging_service.py`
        2. Then run the ERP Staging Service: `python erp_staging_service.py`
        3. Next run the Data Warehouse Service: `python data_warehouse_service.py`
        4. Finally run this Dashboard: `streamlit run dashboard.py`
        
        The error is likely because the data warehouse tables and views haven't been created yet.
        """)
        
        # Display connection details
        with st.expander("Database Connection Details"):
            st.code(f"Database URL: {DW_DB_URL}")
            st.markdown("""
            #### Troubleshooting Steps:
            1. Verify that the database connection string is correct
            2. Check that the data warehouse service has run successfully
            3. Ensure that the required tables (dim_customers, dim_products, fact_sales) exist in the database
            """)
            
        # Add a button to retry loading data
        if st.button("Retry Loading Data"):
            st.experimental_rerun()

if __name__ == "__main__":
    main()
