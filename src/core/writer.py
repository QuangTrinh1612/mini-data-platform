from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from deltalake import write_deltalake, DeltaTable

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