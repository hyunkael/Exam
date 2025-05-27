import subprocess
import time
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_service(script_name):
    """Run a Python script and wait for it to complete"""
    logger.info(f"Starting {script_name}...")
    
    try:
        # Run the script and capture output
        process = subprocess.run(['python', script_name], 
                                 check=True, 
                                 stdout=subprocess.PIPE, 
                                 stderr=subprocess.PIPE,
                                 text=True)
        
        logger.info(f"{script_name} completed successfully")
        logger.debug(f"Output: {process.stdout}")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running {script_name}: {e}")
        logger.error(f"Error output: {e.stderr}")
        return False

def main():
    """Run all services in the correct order"""
    logger.info("Starting initialization sequence")
    
    # Step 1: Run CRM Staging Service
    if not run_service('crm_staging_service.py'):
        logger.error("CRM Staging Service failed. Continuing anyway...")
    
    # Step 2: Run ERP Staging Service
    if not run_service('erp_staging_service.py'):
        logger.error("ERP Staging Service failed. Continuing anyway...")
    
    # Step 3: Run Data Warehouse Service
    if not run_service('data_warehouse_service.py'):
        logger.error("Data Warehouse Service failed. Continuing anyway...")
    
    # Step 4: Start Streamlit Dashboard
    logger.info("All initialization complete. Starting Streamlit dashboard...")
    
    # Get the port from environment variable (Render sets this)
    port = os.environ.get('PORT', '8501')
    
    # Start Streamlit (this will block until Streamlit exits)
    try:
        subprocess.run(['streamlit', 'run', 'dashboard.py', 
                       '--server.port', port, 
                       '--server.address', '0.0.0.0'],
                       check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running Streamlit: {e}")

if __name__ == "__main__":
    main()
