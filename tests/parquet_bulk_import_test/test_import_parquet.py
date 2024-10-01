import sys
import os
from jta.utils.decorators import timer
# Add path to the directory containing the DLL
sys.path.append(os.path.abspath(os.path.dirname(os.path.realpath(__file__)) + "/../../src/ParquetBulkImporter/bin/release/net8.0/linux-x64/publish/"))
from pythonnet import load
load("coreclr")
import clr
clr.AddReference('ParquetBulkImporter')

from ParquetBulkImporter import ParquetBulkImporter

folder_path = os.path.abspath(os.path.dirname(os.path.realpath(__file__)) + r"/../resources/")
table_name = "dbo.import_parquet"
connection_string = "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=TestDb;UID=sa;PWD=YourStrong!Passw0rd;TrustServerCertificate=True;"
file_pattern = "*.parquet"
drop_table = True
parallel = 1

importer = ParquetBulkImporter(connectionString=connection_string, folderPath=folder_path, filePattern=file_pattern, tableName=table_name, dropTable=drop_table, parallel=parallel)
importer.Execute().Wait()