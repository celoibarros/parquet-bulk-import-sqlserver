using System;
using System.IO;
using System.Threading.Tasks;
using FluentAssertions;
using Xunit;

namespace ParquetBulkImporter.Tests;

public class ParquetBulkImporterTests
{
    [Fact]
    public async Task Execute_Should_Run_When_IntegrationSettings_ArePresent()
    {
        var connectionString = Environment.GetEnvironmentVariable("SQLSERVER_TEST_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return; // Integration test disabled when env var is not set.
        }

        var folderPath = Environment.GetEnvironmentVariable("PARQUET_TEST_FOLDER")
            ?? Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../tests/resources"));
        var tableName = Environment.GetEnvironmentVariable("PARQUET_TEST_TABLE") ?? "dbo.import_parquet";

        Directory.Exists(folderPath).Should().BeTrue("the parquet test folder must exist");

        var importer = new ParquetBulkImporter(
            connectionString,
            folderPath,
            "*.parquet",
            tableName,
            dropTable: true,
            parallel: 1);

        Func<Task> action = async () => await importer.Execute();
        await action.Should().NotThrowAsync();
    }
}
