using System.Threading.Tasks;
using Xunit;
using FluentAssertions;
using ParquetBulkImporter;
using System;
using System.Diagnostics;

namespace ParquetBulkImporter.Tests
{
    public class ParquetBulkImporterTests
    {
        [Fact]
        public async Task Execute_Should_ExecuteBulkImportSuccessfully()
        {
            // Arrange
            string connectionString = "your-connection-string";
            string folderPath = @"C:\path\to\test\parquet\files"; // Provide a valid folder path for testing
            string filePattern = "*.parquet";
            string tableName = "dbo.TestTable";
            bool dropTable = true;
            int parallel = 2;

            // Instantiate ParquetBulkImporter
            var importer = new ParquetBulkImporter(connectionString, folderPath, filePattern, tableName, dropTable, parallel);

            // Act
            Func<Task> action = async () => await importer.Execute();

            // Assert
            await action.Should().NotThrowAsync(); // Ensures the method does not throw an exception
        }

        static async Task Main(string[] args)
        {
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            string folderPath = @"..\resources\";
            string tableName = "dbo.import_parquet";
            string connectionString = "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=TestDb;UserId=sa;Password=YourStrong!Passw0rd;";
            string filePattern = "*.parquet";
            bool dropTable = true;
            int parallel = 1;

            var importer = new ParquetBulkImporter(connectionString: connectionString, folderPath: folderPath, filePattern:filePattern, tableName: tableName, dropTable: dropTable, parallel);
            await importer.Execute();
            
            stopWatch.Stop();

            Console.WriteLine("Import completed.");
            Console.WriteLine("Elapsed time {0} ms", stopWatch.Elapsed);
        }
    }
}