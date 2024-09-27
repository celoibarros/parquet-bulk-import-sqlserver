using Parquet.Data;
using Parquet.Schema;
using System.Data;

namespace ParquetBulkImporter.Models
{
    public class ParquetDataReader : IDataReader
    {
        private readonly List<DataField> _fields;
        private readonly Dictionary<string, List<object>> _data;
        private int _currentRowIndex = -1;

        public ParquetDataReader(ParquetSchema schema, Dictionary<string, List<object>> data)
        {
            _fields = new List<DataField>(schema.DataFields);
            _data = data;
        }

        public int FieldCount => _fields.Count;

        public object GetValue(int i)
        {
            string columnName = _fields[i].Name;
            return _data[columnName][_currentRowIndex];
        }

        public bool Read()
        {
            _currentRowIndex++;
            return _currentRowIndex < _data[_fields[0].Name].Count;
        }

        public string GetName(int i) => _fields[i].Name;
        public int GetOrdinal(string name) => _fields.FindIndex(f => f.Name == name);
        public bool IsDBNull(int i) => GetValue(i) == null;
        public string GetDataTypeName(int i) => _fields[i].ClrType.Name;
        public Type GetFieldType(int i) => _fields[i].ClrType;
        public object this[int i] => GetValue(i);
        public object this[string name] => GetValue(GetOrdinal(name));

        public void Dispose() { /* Clean up resources if needed */ }
        public bool NextResult() => false;
        public int Depth => 0;
        public bool IsClosed => false;
        public int RecordsAffected => 0;
        public void Close() { }
        public int GetValues(object[] values) => throw new NotImplementedException();
        public bool GetBoolean(int i) => (bool)GetValue(i);
        public byte GetByte(int i) => (byte)GetValue(i);
        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => throw new NotImplementedException();
        public char GetChar(int i) => (char)GetValue(i);
        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => throw new NotImplementedException();
        public Guid GetGuid(int i) => (Guid)GetValue(i);
        public short GetInt16(int i) => (short)GetValue(i);
        public int GetInt32(int i) => (int)GetValue(i);
        public long GetInt64(int i) => (long)GetValue(i);
        public float GetFloat(int i) => (float)GetValue(i);
        public double GetDouble(int i) => (double)GetValue(i);
        public string GetString(int i) => (string)GetValue(i);
        public decimal GetDecimal(int i) => (decimal)GetValue(i);
        public DateTime GetDateTime(int i) => (DateTime)GetValue(i);
        public IDataReader GetData(int i) => throw new NotImplementedException();
    }
}
