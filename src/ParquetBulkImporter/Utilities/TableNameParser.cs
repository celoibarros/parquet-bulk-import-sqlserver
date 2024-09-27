using System.Text.RegularExpressions;
using ParquetBulkImporter.Models;

namespace ParquetBulkImporter.Utilities
{
    public static class TableNameParser
    {
        public static TableNameParts? ParseTableName(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) return null;

            var pattern = @"^(?:(\[(?<catalog>[^\[\]]+)\]|\b(?<catalog>[a-zA-Z_][a-zA-Z0-9_]*)\b)\.)?" +
                          @"(?:(\[(?<schema>[^\[\]]+)\]|\b(?<schema>[a-zA-Z_][a-zA-Z0-9_]*)\b)\.)?" +
                          @"(\[(?<table>[^\[\]]+)\]|\b(?<table>[a-zA-Z_][a-zA-Z0-9_]*)\b)$";

            var regex = new Regex(pattern, RegexOptions.IgnoreCase);
            var match = regex.Match(tableName);

            if (!match.Success) return null;

            var catalog = match.Groups["catalog"].Success ? match.Groups["catalog"].Value : null;
            var schema = match.Groups["schema"].Success ? match.Groups["schema"].Value : null;
            var table = match.Groups["table"].Value;

            schema ??= "dbo";
            return new TableNameParts { Catalog = catalog, Schema = schema, Name = table };
        }
    }
}
