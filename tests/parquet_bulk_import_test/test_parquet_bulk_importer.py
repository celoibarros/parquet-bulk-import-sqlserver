import clr
import unittest

# Add reference to the .NET DLL
clr.AddReference(r"path_to_dll/ParquetBulkImporter.dll")

# Import the namespace and class
from ParquetBulkImporter import ParquetBulkImporter

class TestParquetBulkImporter(unittest.TestCase):
    def setUp(self):
        self.connection_string = "your-connection-string"
        self.folder_path = "path_to_test_files"  # Path to your parquet files
        self.file_pattern = "*.parquet"
        self.table_name = "dbo.TestTable"
        self.importer = ParquetBulkImporter(self.connection_string, False, 2)

    def test_bulk_import(self):
        # Ensure no exceptions occur during import
        try:
            self.importer.BulkImportAsync(self.folder_path, self.file_pattern, self.table_name)
        except Exception as e:
            self.fail(f"BulkImportAsync raised an exception {e}")

if __name__ == '__main__':
    unittest.main()