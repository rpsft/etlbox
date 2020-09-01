using ETLBox.DataFlow;
using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace ETLBox.ControlFlow
{
    /// <inheritdoc/>
    public class TableData : TableData<object[]>
    {
        public TableData(TableDefinition definition) : base(definition) { }
        public TableData(TableDefinition definition, int estimatedBatchSize) : base(definition, estimatedBatchSize) { }
    }

    /// <summary>
    /// Defines a list of rows that can be inserted into a table
    /// </summary>
    /// <typeparam name="T">Object type of a row</typeparam>
    public class TableData<T> : ITableData
    {
        #region ITableData implementation
        /// <inheritdoc/>
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

        /// <inheritdoc/>
        public List<object[]> Rows { get; set; }

        #endregion

        #region IDataReader Implementation

        /// <inheritdoc/>
        public object this[string name] => Rows[GetOrdinal(name)];
        /// <inheritdoc/>
        public object this[int i] => Rows[i];
        /// <inheritdoc/>
        public int Depth => 0;
        /// <inheritdoc/>
        public int FieldCount => Rows.Count;
        /// <inheritdoc/>
        public bool IsClosed => Rows.Count == 0;
        /// <inheritdoc/>
        public int RecordsAffected => Rows.Count;
        /// <inheritdoc/>
        public bool GetBoolean(int i) => Convert.ToBoolean(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        /// <inheritdoc/>
        public byte GetByte(int i) => Convert.ToByte(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        /// <inheritdoc/>
        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => 0;
        /// <inheritdoc/>
        public char GetChar(int i) => Convert.ToChar(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        /// <inheritdoc/>
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            string value = Convert.ToString(CurrentRow[ShiftIndexAroundIDColumn(i)]);
            buffer = value.Substring(bufferoffset, length).ToCharArray();
            return buffer.Length;
        }
        /// <inheritdoc/>
        public DateTime GetDateTime(int i) => Convert.ToDateTime(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        /// <inheritdoc/>
        public IDataReader GetData(int i) => throw new NotImplementedException();//null;
        /// <inheritdoc/>
        public decimal GetDecimal(int i) => Convert.ToDecimal(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        /// <inheritdoc/>
        public double GetDouble(int i) => Convert.ToDouble(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        /// <inheritdoc/>
        public float GetFloat(int i) => float.Parse(Convert.ToString(CurrentRow[ShiftIndexAroundIDColumn(i)]));
        /// <inheritdoc/>
        public Guid GetGuid(int i) => Guid.Parse(Convert.ToString(CurrentRow[ShiftIndexAroundIDColumn(i)]));
        /// <inheritdoc/>
        public short GetInt16(int i) => Convert.ToInt16(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        /// <inheritdoc/>
        public int GetInt32(int i) => Convert.ToInt32(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        /// <inheritdoc/>
        public long GetInt64(int i) => Convert.ToInt64(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        /// <inheritdoc/>
        public string GetName(int i) => throw new NotImplementedException();
        /// <inheritdoc/>
        public string GetDataTypeName(int i) => throw new NotImplementedException();
        /// <inheritdoc/>
        public Type GetFieldType(int i) => throw new NotImplementedException();
        /// <inheritdoc/>
        public int GetOrdinal(string name) => FindOrdinalInObject(name);

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public string GetString(int i) => Convert.ToString(CurrentRow[ShiftIndexAroundIDColumn(i)]);
        public object GetValue(int i) => CurrentRow.Length > ShiftIndexAroundIDColumn(i) ? CurrentRow[ShiftIndexAroundIDColumn(i)] : (object)null;

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

        #endregion

        #region Constructors

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

        #endregion

        object[] CurrentRow;
        internal Dictionary<string, int> DynamicColumnNames { get; set; } = new Dictionary<string, int>();
        int ReadIndex;
        TableDefinition Definition;
        bool HasDefinition => Definition != null;
        DBTypeInfo TypeInfo;
        int? IDColumnIndex;
        bool HasIDColumnIndex => IDColumnIndex != null;
        private int FindOrdinalInObject(string name)
        {
            if (TypeInfo == null || TypeInfo.IsArray)
            {
                return Definition.Columns.FindIndex(col => col.Name == name);
            }
            else if (TypeInfo.IsDynamic)
            {
                int ix = DynamicColumnNames[name];
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

        int ShiftIndexAroundIDColumn(int i)
        {
            if (HasIDColumnIndex)
            {
                if (i > IDColumnIndex) return i - 1;
                else if (i <= IDColumnIndex) return i;
            }
            return i;
        }


        /// <summary>
        /// Clears the internal list that holds the data and rewinds the pointer for the reader to the start
        /// </summary>
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

        /// Diposes the internal list that holds data
        public void Dispose()
        {
            Dispose(true);
        }

        /// Diposes the internal list that holds data
        public void Close()
        {
            Dispose();
        }

        #endregion
    }
}
