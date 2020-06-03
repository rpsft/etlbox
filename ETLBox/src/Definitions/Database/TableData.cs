using ETLBox.DataFlow;
using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace ETLBox.ControlFlow
{
    public class TableData : TableData<object[]>
    {
        public TableData(TableDefinition definition) : base(definition) { }
        public TableData(TableDefinition definition, int estimatedBatchSize) : base(definition, estimatedBatchSize) { }
    }

    public class TableData<T> : ITableData
    {
        public IColumnMappingCollection ColumnMapping
        {
            get
            {
                if (HasDefinition)
                    return GetColumnMappingFromDefinition();
                else
                    throw new ETLBoxException("No table definition found. For Bulk insert a TableDefinition is always needed.");
            }
        }

        private IColumnMappingCollection GetColumnMappingFromDefinition()
        {
            var mapping = new DataColumnMappingCollection();
            foreach (var col in Definition.Columns)
                if (!col.IsIdentity)
                {
                    if (TypeInfo != null && !TypeInfo.IsDynamic && !TypeInfo.IsArray)
                    {
                        if (TypeInfo.HasPropertyOrColumnMapping(col.Name))
                            mapping.Add(new DataColumnMapping(col.SourceColumn, col.DataSetColumn));
                    }
                    else if (TypeInfo.IsDynamic)
                    {
                        if (DynamicColumnNames.ContainsKey(col.Name))
                            mapping.Add(new DataColumnMapping(col.SourceColumn, col.DataSetColumn));
                    }
                    else
                    {
                        mapping.Add(new DataColumnMapping(col.SourceColumn, col.DataSetColumn));
                    }
                }
            return mapping;
        }

        public List<object[]> Rows { get; set; }
        public object[] CurrentRow { get; set; }
        public Dictionary<string, int> DynamicColumnNames { get; set; } = new Dictionary<string, int>();
        int ReadIndex { get; set; }
        TableDefinition Definition { get; set; }
        bool HasDefinition => Definition != null;
        DBTypeInfo TypeInfo { get; set; }
        int? IDColumnIndex { get; set; }
        bool HasIDColumnIndex => IDColumnIndex != null;

        public TableData(TableDefinition definition)
        {
            InitObjects(definition);
        }

        public TableData(TableDefinition definition, int estimatedBatchSize)
        {
            InitObjects(definition, estimatedBatchSize);
        }

        private void InitObjects(TableDefinition definition, int estimatedBatchSize = 0)
        {
            Definition = definition;
            IDColumnIndex = Definition.IDColumnIndex;
            Rows = new List<object[]>(estimatedBatchSize);
            TypeInfo = new DBTypeInfo(typeof(T));
        }

        public object this[string name] => Rows[GetOrdinal(name)];
        public object this[int i] => Rows[i];
        public int Depth => 0;
        public int FieldCount => Rows.Count;
        public bool IsClosed => Rows.Count == 0;
        public int RecordsAffected => Rows.Count;
        public bool GetBoolean(int i) => Convert.ToBoolean(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public byte GetByte(int i) => Convert.ToByte(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => 0;
        public char GetChar(int i) => Convert.ToChar(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            string value = Convert.ToString(CurrentRow[ShiftIndexAroundIDColumn(i)]);
            buffer = value.Substring(bufferoffset, length).ToCharArray();
            return buffer.Length;

        }
        public DateTime GetDateTime(int i) => Convert.ToDateTime(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public IDataReader GetData(int i) => throw new NotImplementedException();//null;
        public decimal GetDecimal(int i) => Convert.ToDecimal(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public double GetDouble(int i) => Convert.ToDouble(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public float GetFloat(int i) => float.Parse(Convert.ToString(CurrentRow[ShiftIndexAroundIDColumn(i)]));
        public Guid GetGuid(int i) => Guid.Parse(Convert.ToString(CurrentRow[ShiftIndexAroundIDColumn(i)]));
        public short GetInt16(int i) => Convert.ToInt16(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public int GetInt32(int i) => Convert.ToInt32(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public long GetInt64(int i) => Convert.ToInt64(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public string GetName(int i) => throw new NotImplementedException();
        public string GetDataTypeName(int i) => throw new NotImplementedException();
        public Type GetFieldType(int i) => throw new NotImplementedException();

        public int GetOrdinal(string name) => FindOrdinalInObject(name);

        private int FindOrdinalInObject(string name)
        {
            if (TypeInfo == null || TypeInfo.IsArray)
            {
                return Definition.Columns.FindIndex(col => col.Name == name);
            }
            else if (TypeInfo.IsDynamic)
            {
                int ix = DynamicColumnNames[name];//.FindIndex(n =>  n == name);
                if (HasIDColumnIndex)
                    if (ix >= IDColumnIndex) ix++;
                return ix;

            }
            else
            {
                int ix = TypeInfo.GetIndexByPropertyNameOrColumnMapping(name);
                if (HasIDColumnIndex)
                    if (ix >= IDColumnIndex) ix++;
                return ix;
            }
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        //public string GetDestinationDataType(int i) => Definition.Columns[ShiftIndexAroundIDColumn(i)].DataType;
        //public System.Type GetDestinationNETDataType(int i) => Definition.Columns[ShiftIndexAroundIDColumn(i)].NETDataType;

        public string GetString(int i) => Convert.ToString(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public object GetValue(int i) => CurrentRow.Length > ShiftIndexAroundIDColumn(i) ? CurrentRow[ShiftIndexAroundIDColumn(i)] : (object)null;

        int ShiftIndexAroundIDColumn(int i)
        {
            if (HasIDColumnIndex)
            {
                if (i > IDColumnIndex) return i - 1;
                else if (i <= IDColumnIndex) return i;
            }
            return i;
        }

        public int GetValues(object[] values)
        {
            values = CurrentRow as object[];
            return values.Length;
        }

        public bool IsDBNull(int i)
        {
            return CurrentRow.Length > ShiftIndexAroundIDColumn(i) ?
                CurrentRow[ShiftIndexAroundIDColumn(i)] == null : true;
        }

        public bool NextResult()
        {
            return (ReadIndex + 1) <= Rows?.Count;
        }

        public bool Read()
        {
            if (Rows?.Count > ReadIndex)
            {
                CurrentRow = Rows[ReadIndex];
                ReadIndex++;
                return true;
            }
            else
                return false;
        }

        public void ClearData()
        {
            ReadIndex = 0;
            CurrentRow = null;
            Rows.Clear();
        }

        #region IDisposable Support
        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Rows.Clear();
                    Rows = null;
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public void Close()
        {
            Dispose();
        }
        #endregion
    }
}
