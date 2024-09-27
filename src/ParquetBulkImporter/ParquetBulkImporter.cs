using ParquetBulkImporter.Services;
using System.CommandLine;

namespace ParquetBulkImporter
{
    public class Program
    {
        public static async Task<int> Main(string[] args)
        {
            // Define command line options
            var connectionStringOption = new Option<string>(
                name: "--connection-string",
                description: "SQL Server connection string",
                isRequired: true
            );

            var folderPathOption = new Option<string>(
                name: "--folder-path",
                description: "Path to folder containing Parquet files",
                isRequired: true
            );

            var filePatternOption = new Option<string>(
                name: "--file-pattern",
                description: "File pattern to search for Parquet files (e.g., *.parquet)",
                isRequired: true
            );

            var tableNameOption = new Option<string>(
                name: "--table-name",
                description: "Destination table name",
                isRequired: true
            );

            var dropTableOption = new Option<bool>(
                name: "--drop-table",
                description: "Drop the destination table if it exists",
                getDefaultValue: () => false
            );

            var parallelOption = new Option<int>(
                name: "--parallel",
                description: "Number of parallel tasks for importing files",
                getDefaultValue: () => 1
            );

            // Create a root command with the defined options
            var rootCommand = new RootCommand
            {
                connectionStringOption,
                folderPathOption,
                filePatternOption,
                tableNameOption,
                dropTableOption,
                parallelOption
            };

            rootCommand.Description = "Parquet bulk import tool for SQL Server";

            rootCommand.SetHandler(
                async (string connectionString, string folderPath, string filePattern, string tableName, bool dropTable, int parallel) =>
                {
                    // Execute bulk import logic
                    var importer = new ParquetBulkImporter(connectionString, dropTable, parallel);
                    try
                    {
                        await importer.BulkImportAsync(folderPath, filePattern, tableName);
                        Console.WriteLine("Bulk import completed successfully.");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"An error occurred: {ex.Message}");
                    }
                },
                connectionStringOption, folderPathOption, filePatternOption, tableNameOption, dropTableOption, parallelOption
            );

            // Invoke the command handler
            return await rootCommand.InvokeAsync(args);
        }
    }
}