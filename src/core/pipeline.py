import requests
import pandas as pd
from deltalake import write_deltalake, DeltaTable
from datetime import datetime
import os
from loguru import logger

class TreasuryDataPipeline:
    def __init__(self, workspace_name, lakehouse_name):
        """
        Initialize the data pipeline
        
        Args:
            workspace_name: Fabric workspace name
            lakehouse_name: Lakehouse name in Fabric
        """
        self.workspace_name = workspace_name
        self.lakehouse_name = lakehouse_name
        self.api_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/rates_of_exchange"
        
        # OneLake path format: abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse
        self.onelake_path = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}"
        
    def fetch_treasury_data(self, page_size=1000, max_pages=None):
        """
        Fetch data from Treasury API with pagination support
        
        Args:
            page_size: Number of records per page
            max_pages: Maximum number of pages to fetch (None for all)
        
        Returns:
            DataFrame with all fetched data
        """
        all_data = []
        page = 1
        
        while True:
            if max_pages and page > max_pages:
                break
                
            params = {
                'page[size]': page_size,
                'page[number]': page
            }
            
            logger.info(f"Fetching page {page}...")
            
            try:
                response = requests.get(self.api_url, params=params)
                response.raise_for_status()
                
                data = response.json()
                
                if 'data' not in data or len(data['data']) == 0:
                    logger.info("No more data to fetch")
                    break
                
                all_data.extend(data['data'])
                
                # Check if there are more pages
                if 'links' in data and 'next' not in data['links']:
                    break
                    
                page += 1
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data: {e}")
                raise
        
        logger.info(f"Fetched {len(all_data)} records")
        return pd.DataFrame(all_data)
    
    def transform_data(self, df):
        """
        Transform and clean the data
        
        Args:
            df: Raw DataFrame from API
        
        Returns:
            Transformed DataFrame
        """
        # Convert record_date to datetime
        df['record_date'] = pd.to_datetime(df['record_date'])
        
        # Convert exchange_rate to float
        df['exchange_rate'] = pd.to_numeric(df['exchange_rate'], errors='coerce')
        
        # Add processing metadata
        df['processed_timestamp'] = datetime.now()
        df['source_system'] = 'treasury_api'
        
        # Add year and month for partitioning
        df['year'] = df['record_date'].dt.year
        df['month'] = df['record_date'].dt.month
        
        # Select and order columns
        columns = [
            'record_date', 'country', 'currency', 'exchange_rate',
            'country_currency_desc', 'effective_date',
            'year', 'month', 'processed_timestamp', 'source_system'
        ]
        
        # Keep only columns that exist in the dataframe
        columns = [col for col in columns if col in df.columns]
        
        return df[columns]
    
    def write_to_onelake_delta(self, df, table_name, mode='overwrite', partition_cols=['year', 'month']):
        """
        Write DataFrame to OneLake in Delta format using delta-rs
        
        Args:
            df: DataFrame to write
            table_name: Name of the Delta table
            mode: Write mode ('overwrite' or 'append')
            partition_cols: Columns to partition by
        """
        # Construct the full path for the Delta table
        delta_path = f"{self.onelake_path}/Tables/{table_name}"
        
        logger.info(f"Writing to Delta table at: {delta_path}")
        
        try:
            # Configure storage options for Azure
            storage_options = {
                # "azure_storage_account_name": "onelake",
                # "azure_storage_sas_token": os.environ.get("AZURE_SAS_TOKEN", ""),
                # Alternative: Use Azure AD authentication
                "azure_use_azure_active_directory": "true"
            }
            
            # Write to Delta format
            write_deltalake(
                delta_path,
                df,
                mode=mode,
                partition_by=partition_cols,
                storage_options=storage_options,
                engine='pyarrow',
                schema_mode='merge'  # Allows schema evolution
            )
            
            logger.info(f"Successfully wrote {len(df)} records to Delta table")
            
            # Verify the write by reading table metadata
            dt = DeltaTable(delta_path, storage_options=storage_options)
            logger.info(f"Delta table version: {dt.version()}")
            logger.info(f"Number of files: {len(dt.files())}")
            
        except Exception as e:
            logger.error(f"Error writing to Delta table: {e}")
            raise
    
    def run_pipeline(self, table_name='exchange_rates', max_pages=5):
        """
        Run the complete data pipeline
        
        Args:
            table_name: Name of the target Delta table
            max_pages: Maximum pages to fetch (None for all)
        """
        logger.info("Starting Treasury data pipeline...")
        
        # Step 1: Fetch data
        logger.info("Fetching data from Treasury API...")
        df = self.fetch_treasury_data(max_pages=max_pages)
        
        # Step 2: Transform data
        logger.info("Transforming data...")
        df_transformed = self.transform_data(df)
        
        # Log sample of transformed data
        logger.info(f"Transformed data shape: {df_transformed.shape}")
        logger.info(f"Columns: {df_transformed.columns.tolist()}")
        logger.info(f"Sample data:\n{df_transformed.head()}")
        
        # Step 3: Write to OneLake
        logger.info("Writing to OneLake in Delta format...")
        self.write_to_onelake_delta(df_transformed, table_name)
        
        logger.info("Pipeline completed successfully!")
        
        return df_transformed