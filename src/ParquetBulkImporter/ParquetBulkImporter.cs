using ParquetBulkImporter.Services;
using System.Threading.Tasks;

namespace ParquetBulkImporter
{
    public class ParquetBulkImporter
    {
        private readonly string _folderPath;
        private readonly string _filePattern;
        private readonly string _tableName;
        private readonly Services.ParquetBulkImporter _importer;

         public ParquetBulkImporter(string connectionString, string folderPath, string filePattern, string tableName, bool dropTable=false, int parallel=1)
         {
            _importer = new Services.ParquetBulkImporter(connectionString, dropTable, parallel);
            _folderPath = folderPath;
            _filePattern = filePattern;
            _tableName = tableName;
            
        }

        public async Task Execute(){
            await _importer.BulkImportAsync(_folderPath, _filePattern, _tableName);
        }
    }
}