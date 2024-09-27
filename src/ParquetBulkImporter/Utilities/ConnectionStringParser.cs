namespace ParquetBulkImporter.Utilities
{
    public static class ConnectionStringParser
    {
        public static Dictionary<string, string> ParseOdbcConnectionString(string connectionString)
        {
            var connectionParams = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var keyValues = connectionString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);

            foreach (var keyValue in keyValues)
            {
                var splitKeyValue = keyValue.Split(new[] { '=' }, 2);
                if (splitKeyValue.Length == 2)
                {
                    connectionParams[splitKeyValue[0].Trim().ToLower()] = splitKeyValue[1].Trim();
                }
            }

            return connectionParams;
        }
    }
}
