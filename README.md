# Building a Data Platform: Treasury API to Fabric OneLake with Delta-RS

## Overview

In this lesson, we'll build a data engineering pipeline that:
1. Fetches exchange rate data from the US Treasury API
2. Processes and transforms the data
3. Writes it to Microsoft Fabric OneLake in Delta format using delta-rs

## Prerequisites

- Python 3.8+
- Azure/Microsoft Fabric workspace with OneLake storage
- Basic understanding of REST APIs and data formats

## Step 1: Environment Setup

First, install the required packages:

```bash
pip install deltalake pandas requests pyarrow azure-storage-file-datalake azure-identity
```

## Step 2: Understanding the Treasury API

The Treasury API endpoint provides exchange rate data:
- **Endpoint**: `https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/rates_of_exchange`
- **Format**: JSON
- **Key fields**: 
  - `record_date`: Date of the exchange rate
  - `country`: Country name
  - `currency`: Currency name
  - `exchange_rate`: Exchange rate value

## Step 3: Complete Implementation

```python
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake, DeltaTable
from datetime import datetime
import os
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
                "azure_storage_account_name": "onelake",
                "azure_storage_sas_token": os.environ.get("AZURE_SAS_TOKEN", ""),
                # Alternative: Use Azure AD authentication
                # "azure_use_azure_active_directory": "true"
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

# Alternative implementation using ADLS Gen2 APIs directly
class OneLakeDeltaWriter:
    """
    Alternative approach using Azure Data Lake Storage Gen2 APIs
    """
    def __init__(self, workspace_name, lakehouse_name):
        self.workspace_name = workspace_name
        self.lakehouse_name = lakehouse_name
        self.account_name = "onelake"
        self.file_system = workspace_name
        
        # Initialize Azure credentials
        self.credential = DefaultAzureCredential()
        
        # Create DataLake service client
        self.service_client = DataLakeServiceClient(
            account_url=f"https://{self.account_name}.dfs.fabric.microsoft.com",
            credential=self.credential
        )
    
    def write_delta_with_adls(self, df, table_path):
        """
        Write Delta table using ADLS Gen2 APIs
        """
        # Get file system client
        file_system_client = self.service_client.get_file_system_client(
            file_system=self.file_system
        )
        
        # Construct full path
        full_path = f"{self.lakehouse_name}/Tables/{table_path}"
        
        # Write using delta-rs with ADLS storage options
        storage_options = {
            "account_name": self.account_name,
            "use_azure_active_directory": "true"
        }
        
        write_deltalake(
            f"abfss://{self.file_system}@{self.account_name}.dfs.fabric.microsoft.com/{full_path}",
            df,
            storage_options=storage_options
        )

# Usage example
if __name__ == "__main__":
    # Configure your Fabric workspace and lakehouse names
    WORKSPACE_NAME = "your-workspace-name"
    LAKEHOUSE_NAME = "your-lakehouse-name"
    
    # Option 1: Set authentication via environment variable
    # os.environ["AZURE_SAS_TOKEN"] = "your-sas-token"
    
    # Option 2: Use Azure AD authentication (recommended)
    # Ensure you're logged in via Azure CLI: az login
    
    # Create and run pipeline
    pipeline = TreasuryDataPipeline(WORKSPACE_NAME, LAKEHOUSE_NAME)
    
    # Run the pipeline (fetching only 5 pages for demo)
    result_df = pipeline.run_pipeline(
        table_name='treasury_exchange_rates',
        max_pages=5
    )
    
    # Display summary statistics
    print(f"\nPipeline Summary:")
    print(f"Total records processed: {len(result_df)}")
    print(f"Date range: {result_df['record_date'].min()} to {result_df['record_date'].max()}")
    print(f"Number of unique currencies: {result_df['currency'].nunique()}")
```

## Step 4: Authentication Options

### Option 1: SAS Token
```python
os.environ["AZURE_SAS_TOKEN"] = "your-sas-token-here"
```

### Option 2: Azure AD Authentication (Recommended)
```python
# Login via Azure CLI
# $ az login

# The DefaultAzureCredential will automatically use your Azure CLI credentials
```

### Option 3: Service Principal
```python
os.environ["AZURE_CLIENT_ID"] = "your-client-id"
os.environ["AZURE_CLIENT_SECRET"] = "your-client-secret"
os.environ["AZURE_TENANT_ID"] = "your-tenant-id"
```

## Step 5: Advanced Features

### Incremental Loading
```python
def incremental_load(self, table_name, last_loaded_date=None):
    """
    Perform incremental load based on record_date
    """
    # Read existing Delta table to get max date
    if last_loaded_date is None:
        try:
            dt = DeltaTable(f"{self.onelake_path}/Tables/{table_name}")
            max_date_df = dt.to_pandas()[['record_date']].max()
            last_loaded_date = max_date_df['record_date']
        except:
            last_loaded_date = '2020-01-01'
    
    # Fetch only new records
    params = {
        'filter': f'record_date:gt:{last_loaded_date}',
        'page[size]': 1000
    }
    
    # Fetch and append new data
    new_data = self.fetch_treasury_data(params=params)
    if not new_data.empty:
        transformed = self.transform_data(new_data)
        self.write_to_onelake_delta(transformed, table_name, mode='append')
```

### Data Quality Checks
```python
def validate_data(self, df):
    """
    Perform data quality checks
    """
    issues = []
    
    # Check for nulls in critical columns
    null_checks = ['record_date', 'country', 'currency', 'exchange_rate']
    for col in null_checks:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            issues.append(f"{col} has {null_count} null values")
    
    # Check for invalid exchange rates
    invalid_rates = df[df['exchange_rate'] <= 0]
    if len(invalid_rates) > 0:
        issues.append(f"{len(invalid_rates)} invalid exchange rates found")
    
    # Check date ranges
    if df['record_date'].max() > datetime.now():
        issues.append("Future dates found in record_date")
    
    return issues
```

### Delta Table Optimization
```python
def optimize_delta_table(self, table_name):
    """
    Optimize Delta table for better performance
    """
    delta_path = f"{self.onelake_path}/Tables/{table_name}"
    dt = DeltaTable(delta_path)
    
    # Compact small files
    dt.optimize.compact()
    
    # Z-order by frequently queried columns
    dt.optimize.z_order(['country', 'currency', 'record_date'])
    
    # Vacuum old files (retain 7 days of history)
    dt.vacuum(retention_hours=168)
```

## Step 6: Scheduling and Orchestration

### Using Azure Data Factory
```json
{
  "name": "TreasuryDataPipeline",
  "properties": {
    "activities": [{
      "name": "RunPythonScript",
      "type": "PythonActivity",
      "typeProperties": {
        "scriptPath": "pipeline/treasury_pipeline.py",
        "scriptArguments": ["--workspace", "your-workspace", "--lakehouse", "your-lakehouse"]
      }
    }],
    "triggers": [{
      "name": "DailyTrigger",
      "type": "ScheduleTrigger",
      "typeProperties": {
        "recurrence": {
          "frequency": "Day",
          "interval": 1,
          "startTime": "2024-01-01T00:00:00Z"
        }
      }
    }]
  }
}
```

### Using Apache Airflow
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'treasury_data_pipeline',
    default_args=default_args,
    description='Load Treasury exchange rates to OneLake',
    schedule_interval='@daily',
    catchup=False
)

def run_pipeline(**context):
    pipeline = TreasuryDataPipeline("workspace", "lakehouse")
    pipeline.run_pipeline()

load_task = PythonOperator(
    task_id='load_treasury_data',
    python_callable=run_pipeline,
    dag=dag
)
```

## Best Practices

1. **Error Handling**: Always implement proper error handling and logging
2. **Data Validation**: Validate data before writing to OneLake
3. **Incremental Loading**: Use incremental loading for large datasets
4. **Partitioning**: Partition by date columns for better query performance
5. **Schema Evolution**: Use `schema_mode='merge'` to handle schema changes
6. **Monitoring**: Implement monitoring and alerting for pipeline failures
7. **Testing**: Write unit tests for transformation logic

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Ensure Azure CLI is logged in: `az login`
   - Check if service principal has correct permissions

2. **Network Errors**
   - Verify firewall rules allow access to OneLake
   - Check if VPN is required for your organization

3. **Delta Write Errors**
   - Ensure the lakehouse path exists
   - Verify write permissions on the target location

4. **Memory Issues**
   - Use chunking for large datasets
   - Consider using Spark for very large data volumes

## Conclusion

You now have a complete data pipeline that:
- Fetches data from the Treasury API
- Transforms and validates the data
- Writes to Microsoft Fabric OneLake in Delta format
- Handles errors and provides logging
- Supports incremental loading and optimization

This foundation can be extended with additional features like data quality monitoring, automated testing, and integration with other data sources.