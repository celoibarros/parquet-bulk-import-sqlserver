namespace ParquetBulkImporter.Models
{
    public class TableNameParts
    {
        public string? Catalog { get; set; }
        public string Schema { get; set; } = "dbo"; // Default value
        public string Name { get; set; } = string.Empty;

        public string GetFullTableName()
        {
            if (string.IsNullOrEmpty(Catalog))
            {
                return $"{Schema}.{Name}";
            }
            return $"{Catalog}.{Schema}.{Name}";
        }
    }
}
