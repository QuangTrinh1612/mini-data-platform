from dotenv import load_dotenv
from src.core.pipeline import TreasuryDataPipeline

load_dotenv()

def main():
    # Configure your Fabric workspace and lakehouse names
    WORKSPACE_NAME = "Oracle_ETL"
    LAKEHOUSE_NAME = "Oracle_LKH.Lakehouse"
    
    # Option 1: Set authentication via environment variable
    # os.environ["AZURE_SAS_TOKEN"] = "your-sas-token"
    
    # Option 2: Use Azure AD authentication (recommended)
    # Ensure you're logged in via Azure CLI: az login
    
    # Create and run pipeline
    pipeline = TreasuryDataPipeline(WORKSPACE_NAME, LAKEHOUSE_NAME)
    
    # Run the pipeline (fetching only 5 pages for demo)
    result_df = pipeline.run_pipeline(
        schema_name='bronze_sftp',
        table_name='treasury_exchange_rates',
        max_pages=5
    )
    
    # Display summary statistics
    print(f"\nPipeline Summary:")
    print(f"Total records processed: {len(result_df)}")
    print(f"Date range: {result_df['record_date'].min()} to {result_df['record_date'].max()}")
    print(f"Number of unique currencies: {result_df['currency'].nunique()}")

if __name__ == '__main__':
    main()