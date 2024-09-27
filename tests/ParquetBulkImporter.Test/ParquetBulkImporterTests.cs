using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

namespace ParquetBulkImporter.Tests
{
    public class ParquetBulkImporterTests
    {
        [Fact]
        public async Task Should_ImportFilesCorrectly()
        {
            // Arrange
            string connectionString = "your-connection-string";
            string folderPath = @"path_to_test_files"; // Path to your test parquet files
            string filePattern = "*.parquet";
            string tableName = "dbo.TestTable";
            var importer = new ParquetBulkImporter(connectionString, dropTable: true, parallel: 2);

            // Act
            Func<Task> action = async () => await importer.BulkImportAsync(folderPath, filePattern, tableName);

            // Assert
            await action.Should().NotThrowAsync(); // Asserts that the import completes without throwing an exception
        }
    }
}
