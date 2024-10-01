using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using System.Data;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using ParquetBulkImporter.Models;
using ParquetBulkImporter.Utilities;

namespace ParquetBulkImporter.Services
{
    public class ParquetBulkImporter
    {
        private readonly string _connectionString;
        private readonly string? _accessToken;
        private readonly bool _dropTable;
        private readonly int _parallel;
        private readonly Dictionary<string, string> _parsedConnectionString;

        public ParquetBulkImporter(string connectionString, bool dropTable = false, int parallel = 1)
        {
            _dropTable = dropTable;
            _parallel = parallel;
            _parsedConnectionString = ConnectionStringParser.ParseOdbcConnectionString(connectionString);
            var keysToExclude = new HashSet<string> {"driver", "accesstoken"};

            _connectionString = string.Join(
                ";",
                _parsedConnectionString
                    .Where(kvp => !keysToExclude.Contains(kvp.Key))
                    .Select(kvp => $"{kvp.Key}={kvp.Value}")
            );
            _accessToken = _parsedConnectionString.TryGetValue("accesstoken", out var tokenValue) ? tokenValue : null;
        }

        public async Task BulkImportAsync(string folderPath, string filePattern, string tableName)
        {
            var tableParts = TableNameParser.ParseTableName(tableName);
            if (tableParts == null) throw new ArgumentException("Invalid table name format.", nameof(tableName));

            tableParts.Catalog ??= _parsedConnectionString["database"];
            var files = Directory.GetFiles(folderPath, filePattern);
            var tasks = new ConcurrentBag<Task>();

            Console.WriteLine($"Setting up destination table {tableParts.GetFullTableName()}.");
            if (_dropTable) await DropDestinationTable(tableParts);

            if (files.Length > 0)
            {
                var (initialDataReader, _) = await ReadParquetFileAsDataReaderAsync(files.First());
                await SetupDestinationTable(tableParts, initialDataReader);
            }

            using var semaphore = new SemaphoreSlim(_parallel);
            foreach (var file in files)
            {
                await semaphore.WaitAsync();
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        await ProcessFileAsync(file, tableParts);
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }));
            }

            await Task.WhenAll(tasks);
        }

        private async Task BulkInsertAsync(IDataReader dataReader, TableNameParts tableParts)
        {
            string tableName = tableParts.GetFullTableName();
            using (var connection = new SqlConnection(_connectionString))
            {
                if (_accessToken != null)
                {
                    connection.AccessToken = _accessToken;
                }
                await connection.OpenAsync();

                Console.WriteLine($"BulkCopy starting to table {tableName}...");
                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = tableName;
                    bulkCopy.BulkCopyTimeout = 0;

                    for (int i = 0; i < dataReader.FieldCount; i++)
                    {
                        string columnName = dataReader.GetName(i);
                        bulkCopy.ColumnMappings.Add(columnName, columnName);
                    }

                    await bulkCopy.WriteToServerAsync(dataReader);
                }
                Console.WriteLine("BulkCopy Done!");
            }
        }

        private async Task DropDestinationTable(TableNameParts tableParts) { 
            string tableName = tableParts.GetFullTableName();
            using (var connection = new SqlConnection(_connectionString))
            {
                if (_accessToken != null)
                {
                    connection.AccessToken = _accessToken;
                }
                await connection.OpenAsync();

                Console.WriteLine($"Dropping destination table {tableName}.");
                var dropStatement = $"drop table if exists {tableName};";
                using (var command = new SqlCommand(dropStatement, connection))
                {
                    command.CommandTimeout = 60;
                    await command.ExecuteNonQueryAsync();
                } 
            } 
            }
        private async Task SetupDestinationTable(TableNameParts tableParts, IDataReader reader)
        {
            string tableName = tableParts.GetFullTableName();
            using (var connection = new SqlConnection(_connectionString))
            {
                if (_accessToken != null)
                {
                    connection.AccessToken = _accessToken;
                }
                await connection.OpenAsync();
               
                var commandText = GenerateCreateTableCommandFromReader(reader, tableName);
                Console.WriteLine($"Creating table {tableName} with the script: @@{commandText}@@.");
                using (var command = new SqlCommand(commandText, connection))
                {
                    command.CommandTimeout = 60;
                    await command.ExecuteNonQueryAsync();
                }
            }
        }
        private string GenerateCreateTableCommandFromReader(IDataReader reader, string tableName)
        {
            var commandText = $"IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG + '.' + TABLE_SCHEMA + '.' + TABLE_NAME = '{tableName}') ";
            commandText += $"CREATE TABLE {tableName} (";

            for (int i = 0; i < reader.FieldCount; i++)
            {
                string columnName = reader.GetName(i);
                Type fieldType = reader.GetFieldType(i);
                commandText += $"[{columnName}] {GetSqlType(fieldType)},";
            }

            commandText = commandText.TrimEnd(',') + ");";

            return commandText;
        }

        private string GetSqlType(Type type)
        {
            if (type == typeof(int)) return "INT";
            if (type == typeof(long)) return "BIGINT";
            if (type == typeof(float)) return "FLOAT";
            if (type == typeof(double)) return "FLOAT";
            if (type == typeof(bool)) return "BIT";
            if (type == typeof(string)) return "NVARCHAR(MAX)";
            return "NVARCHAR(MAX)";
        }
        private async Task ProcessFileAsync(string file, TableNameParts tableParts)
        {
            var (dataReader, rowCount) = await ReadParquetFileAsDataReaderAsync(file);
            Console.WriteLine($"Importing {file} with {rowCount} rows.");
            await BulkInsertAsync(dataReader, tableParts);
        }
        private async Task<(IDataReader, int)> ReadParquetFileAsDataReaderAsync(string filePath)
        {
            using (ParquetReader parquetReader = await ParquetReader.CreateAsync(filePath))
            {
                ParquetSchema schema = parquetReader.Schema;
                var columnsData = new Dictionary<string, List<object>>();
                int rowCount = 0;

                for (int i = 0; i < parquetReader.RowGroupCount; i++)
                {
                    using (ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(i))
                    {
                        foreach (DataField field in schema.GetDataFields())
                        {
                            Parquet.Data.DataColumn column = await groupReader.ReadColumnAsync(field);
                            string columnName = field.Name;

                            // Initialize column data list if it doesn't exist
                            if (!columnsData.ContainsKey(columnName))
                            {
                                columnsData[columnName] = new List<object>();
                            }

                            // Append column data
                            columnsData[columnName].AddRange(column.Data.Cast<object>());

                            // Update row count based on the current column
                            rowCount = Math.Max(rowCount, columnsData[columnName].Count);
                        }
                    }
                }

                // Make sure each column has the same number of rows
                if (columnsData.Values.Any(colData => colData.Count != rowCount))
                {
                    throw new InvalidOperationException("Inconsistent row counts across columns.");
                }

                return (new ParquetDataReader(schema, columnsData), rowCount);
            }
        }
    }
}
