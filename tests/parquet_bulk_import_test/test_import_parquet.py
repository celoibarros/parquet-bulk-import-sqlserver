import os
import sys
import unittest

try:
    from pythonnet import load
    load("coreclr")
    import clr
    HAS_PYTHONNET = True
except Exception:
    HAS_PYTHONNET = False


class TestImportParquet(unittest.TestCase):
    def test_import_executes_when_configured(self):
        if not HAS_PYTHONNET:
            self.skipTest("pythonnet/coreclr not available")

        connection_string = os.getenv("SQLSERVER_TEST_CONNECTION_STRING")
        if not connection_string:
            self.skipTest("SQLSERVER_TEST_CONNECTION_STRING is not set")

        dll_dir = os.getenv(
            "PARQUET_IMPORTER_DLL_DIR",
            os.path.abspath(
                os.path.join(
                    os.path.dirname(os.path.realpath(__file__)),
                    "../../src/ParquetBulkImporter/bin/Release/net8.0"
                )
            )
        )
        sys.path.append(dll_dir)

        clr.AddReference("ParquetBulkImporter")
        from ParquetBulkImporter import ParquetBulkImporter

        folder_path = os.getenv(
            "PARQUET_TEST_FOLDER",
            os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../resources"))
        )
        table_name = os.getenv("PARQUET_TEST_TABLE", "dbo.import_parquet")

        importer = ParquetBulkImporter(
            connectionString=connection_string,
            folderPath=folder_path,
            filePattern="*.parquet",
            tableName=table_name,
            dropTable=True,
            parallel=1,
        )
        importer.Execute().Wait()


if __name__ == "__main__":
    unittest.main()
