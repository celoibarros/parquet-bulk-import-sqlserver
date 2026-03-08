# Parquet Bulk Import for SQL Server

`ParquetBulkImporter` is a .NET 8 library for high-throughput imports of Parquet files into SQL Server tables.

## Features

- Bulk import from a folder of `.parquet` files
- Optional table drop/recreate flow
- Parallel file processing
- Works on Linux and Windows (runtime: .NET 8)
- Includes C# and Python integration tests

## Requirements

- .NET 8 SDK
- Python 3.10+ (for Python tests)
- SQL Server (local, container, or remote)
- ODBC Driver 18 for SQL Server (for connection string compatibility)

## Quick Start

```bash
git clone https://github.com/<your-username>/parquet-bulk-import-sqlserver.git
cd parquet-bulk-import-sqlserver
dotnet build src/ParquetBulkImporter/ParquetBulkImporter.csproj -c Release
```

## Usage

```csharp
using ParquetBulkImporter;

var importer = new ParquetBulkImporter(
    connectionString: "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=TestDb;UID=sa;PWD=<password>;TrustServerCertificate=True;",
    folderPath: "./tests/resources",
    filePattern: "*.parquet",
    tableName: "dbo.ImportTable",
    dropTable: true,
    parallel: 4
);

await importer.Execute();
```

## Test Configuration

Set these environment variables for integration tests:

- `SQLSERVER_TEST_CONNECTION_STRING`
- `PARQUET_TEST_FOLDER` (optional, default: `tests/resources`)
- `PARQUET_TEST_TABLE` (optional, default: `dbo.import_parquet`)
- `PARQUET_IMPORTER_DLL_DIR` (optional, for Python tests)

### Run .NET tests

```bash
dotnet test tests/ParquetBulkImporterTests/ParquetBulkImporterTests.csproj -c Release
```

### Run Python tests

```bash
python -m pip install -r tests/parquet_bulk_import_test/requirements.txt
python -m unittest discover -s tests/parquet_bulk_import_test -p "test_*.py"
```

## SQL Server via Docker (optional)

```bash
docker compose up -d
```

The default SQL Server endpoint is `localhost,1433` with credentials defined in `docker-compose.yml`.

## Repository Layout

- `src/ParquetBulkImporter`: library source
- `tests/ParquetBulkImporterTests`: C# integration tests
- `tests/parquet_bulk_import_test`: Python integration tests via `pythonnet`
- `tests/resources`: sample parquet file(s)

## License

MIT. See [LICENSE](LICENSE).
