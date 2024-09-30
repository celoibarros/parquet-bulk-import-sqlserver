import clr
import unittest
import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(os.path.realpath(__file__)) + "/../../src/ParquetBulkImporter/bin/release/net8.0/linux-x64/publish/"))
from pythonnet import load
load("coreclr")
import clr
clr.AddReference('ParquetBulkImporter')

class TestParquetBulkImporter(unittest.TestCase):
    def setUp(self):
        self.folder_path = os.path.abspath(os.path.dirname(os.path.realpath(__file__)) + r"/../resources/")
        self.table_name = "dbo.import_parquet"
        self.connection_string = "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=TestDb;UserId=sa;Password=YourStrong!Passw0rd;"
        self.file_pattern = "*.parquet"
        self.drop_table = True
        self.parallel = 1
        self.importer = ParquetBulkImporter(connectionString=self.connection_string, folderPath=self.folder_path, filePattern=self.file_pattern, tableName=self.table_name, dropTable=self.drop_table, parallel=self.parallel)
        

    def test_bulk_import(self):
        try:
            self.importer.Execute().Wait()
        except Exception as e:
            self.fail(f"BulkImportAsync raised an exception {e}")

if __name__ == '__main__':
    unittest.main()