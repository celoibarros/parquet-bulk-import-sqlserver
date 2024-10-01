using System.Text.RegularExpressions;
using ParquetBulkImporter.Models;

namespace ParquetBulkImporter.Utilities
{
    public static class TableNameParser
    {
        public static TableNameParts? ParseTableName(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                return null;
            }

            // Regular expression to capture optional catalog, optional schema, and required table name
            // Supports both bracketed and unbracketed identifiers
            var pattern = @"^(?:(\[(?<catalog>[^\[\]]+)\]|\b(?<catalog>[a-zA-Z_][a-zA-Z0-9_]*)\b)\.)?" +
                        @"(?:(\[(?<schema>[^\[\]]+)\]|\b(?<schema>[a-zA-Z_][a-zA-Z0-9_]*)\b)\.)?" +
                        @"(\[(?<table>[^\[\]]+)\]|\b(?<table>[a-zA-Z_][a-zA-Z0-9_]*)\b)$";

            var regex = new Regex(pattern, RegexOptions.IgnoreCase);
            var match = regex.Match(tableName);

            if (!match.Success)
            {
                return null; // Invalid format
            }

            // Extract matched groups
            string? catalog = match.Groups["catalog"].Success ? match.Groups["catalog"].Value : null;
            string? schema = match.Groups["schema"].Success ? match.Groups["schema"].Value : null;
            string table = match.Groups["table"].Value;

            // Logic adjustment for two-part vs. three-part names
            if (!string.IsNullOrEmpty(catalog) && string.IsNullOrEmpty(schema))
            {
                // For two-part names, treat the first part as schema, not catalog
                schema = catalog;
                catalog = null;
            }

            // Default schema to "dbo" if not specified and only table name is provided
            schema ??= "dbo";

            return new TableNameParts
            {
                Catalog = catalog,
                Schema = schema,
                Name = table
            };
        }
    }
}
