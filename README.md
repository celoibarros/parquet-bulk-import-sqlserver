# Parquet Bulk Import to SQL Server

![Build Status](https://github.com/yourusername/parquet-bulk-import-sqlserver/actions/workflows/ci.yml/badge.svg)

## Overview

`ParquetBulkImporter` is a .NET-based utility designed to bulk import Parquet files into Microsoft SQL Server tables. It allows high-performance ingestion of large datasets with customizable options such as parallel processing, table dropping, and pattern matching for the Parquet files to be imported.

The repository supports running tests using .NET and Python, utilizing `pythonnet` to integrate Python tests with the .NET assembly. Additionally, the repository is set up with GitHub Actions for CI/CD to build the project, run the tests, and package the solution for multiple platforms.

## Features

- **Bulk Import**: Import Parquet files directly into SQL Server.
- **Parallel Processing**: Specify the number of parallel threads for file import.
- **Cross-platform Support**: Target multiple platforms with `net8.0`.
- **Python Interoperability**: Run and test the .NET DLL using Python.
- **SQL Server Docker Support**: The project includes Docker setup for SQL Server testing.

Requirements
.NET 8 SDK
Python 3.x
SQL Server (Local or via Docker)
Docker (for running tests with SQL Server)
pythonnet (for running Python tests)
Installation
1. Clone the Repository
bash
Copy code
git clone https://github.com/yourusername/parquet-bulk-import-sqlserver.git
cd parquet-bulk-import-sqlserver
2. Build the Project
bash
Copy code
dotnet build --configuration Release
3. Publish for Multiple Platforms
bash
Copy code
dotnet publish --configuration Release --output ./publish
This will generate the necessary DLLs in the ./publish directory.

4. Run the Dockerized SQL Server
bash
Copy code
docker-compose up -d
This will set up a local SQL Server instance for testing.

5. Python Setup
Install Python requirements and pythonnet:

bash
Copy code
pip install pythonnet
6. Running Tests
.NET Tests
bash
Copy code
dotnet test tests/ParquetBulkImporterTests
Python Tests
Ensure the .NET assembly is published:

bash
Copy code
python -m unittest discover -s tests/PythonTests -p '*.py'
Usage
To use the bulk importer in your own project, reference the ParquetBulkImporter DLL and instantiate it as follows:

csharp
Copy code
using ParquetBulkImporter;

var importer = new ParquetBulkImporter(
    connectionString: "your-connection-string", 
    folderPath: @"path_to_parquet_files",
    filePattern: "*.parquet", 
    tableName: "dbo.ImportTable",
    dropTable: true, 
    parallel: 4
);
await importer.Execute();
Configuration
You can customize the behavior by modifying the parameters:

connectionString: Your SQL Server connection string.
folderPath: The folder containing Parquet files to import.
filePattern: The pattern to filter the files (e.g., *.parquet).
tableName: The target SQL Server table.
dropTable: Whether to drop the table before importing (default: false).
parallel: Number of parallel operations for file importing (default: 1).
CI/CD with GitHub Actions
The project uses GitHub Actions to build and test the solution. The workflow is defined in .github/workflows/ci.yml.

GitHub Actions Workflow
The CI pipeline includes the following steps:

Build the Project: Builds the solution for multiple platforms.
Run .NET Tests: Executes unit tests.
Run Python Tests: Executes Python tests by importing the .NET DLL.
Package Artifacts: Publishes the compiled DLLs for distribution.
License
This project is licensed under the MIT License. See the LICENSE file for more details.
