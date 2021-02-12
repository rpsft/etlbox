using ETLBox.DataFlow;
using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace ETLBox.ControlFlow
{
    /// <summary>
    /// Defines a list of rows that can be inserted into a table
    /// </summary>
    public class TableData : ITableData
    {
        #region ITableData implementation

        /// <inheritdoc/>
        public IColumnMappingCollection ColumnMapping
        {
            get
            {
                if (_columnMapping == null)
                    _columnMapping = CreateColumnMappingFromDefinition();
                return _columnMapping;
            }
        }

        /// <inheritdoc/>
        public object[] CurrentRow { get; private set; }

        /// <inheritdoc/>
        public int ReadIndex { get; private set; }

        /// <inheritdoc/>
        public List<object[]> Rows { get; set; }

        /// <inheritdoc/>
        public string DestinationTableName => Definition.Name;

        /// <inheritdoc/>
        public string GetDataTypeName(string columnName) =>
            Definition.Columns.Find(col => col.Name == columnName).DataType;

        /// <inheritdoc/>
        public Dictionary<string, int> DataIndexForColumn { get; set; } = new Dictionary<string, int>();

        /// <inheritdoc/>
        public TableDefinition Definition { get; set; }

        /// <inheritdoc/>
        public bool KeepIdentity { get; set; }

        public Dictionary<string, Func<object, object>> ColumnConverters { get; set; } = new Dictionary<string, Func<object, object>>();

        #endregion

        #region IDataReader part I (not needed) 

        /// <inheritdoc/>
        public bool GetBoolean(int i) => Convert.ToBoolean(GetCurrentRow(i));
        /// <inheritdoc/>
        public byte GetByte(int i) => Convert.ToByte(GetCurrentRow(i));
        /// <inheritdoc/>
        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
            => throw new NotImplementedException("GetBytes(..) is not implemented on TableData!");
        /// <inheritdoc/>
        public char GetChar(int i) => Convert.ToChar(GetCurrentRow(i));
        /// <inheritdoc/>
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            string value = Convert.ToString(GetCurrentRow(i));
            buffer = value.Substring(bufferoffset, length).ToCharArray();
            return buffer.Length;
        }
        /// <inheritdoc/>
        public DateTime GetDateTime(int i) => Convert.ToDateTime(GetCurrentRow(i));
        /// <inheritdoc/>
        public IDataReader GetData(int i) => throw new NotImplementedException("GetData(..) is not implemented on TableData!");
        /// <inheritdoc/>
        public decimal GetDecimal(int i) => Convert.ToDecimal(GetCurrentRow(i));
        /// <inheritdoc/>
        public double GetDouble(int i) => Convert.ToDouble(GetCurrentRow(i));
        /// <inheritdoc/>
        public float GetFloat(int i) => float.Parse(Convert.ToString(GetCurrentRow(i)));
        /// <inheritdoc/>
        public Guid GetGuid(int i) => Guid.Parse(Convert.ToString(GetCurrentRow(i)));
        /// <inheritdoc/>
        public short GetInt16(int i) => Convert.ToInt16(GetCurrentRow(i));
        /// <inheritdoc/>
        public int GetInt32(int i) => Convert.ToInt32(GetCurrentRow(i));
        /// <inheritdoc/>
        public long GetInt64(int i) => Convert.ToInt64(GetCurrentRow(i));
        /// <inheritdoc/>
        public string GetString(int i) => Convert.ToString(GetCurrentRow(i));
        /// <inheritdoc/>
        public Type GetFieldType(int i) => GetCurrentRow(i)?.GetType() ?? null;
        /// <inheritdoc/>
        public DataTable GetSchemaTable() //check
            => throw new NotImplementedException("GetSchemaTable() is not implemented on TableData!");
        /// <inheritdoc/>
        public int GetValues(object[] values)
        {
            values = CurrentRow as object[];
            return values?.Length ?? 0;
        }

        #endregion

        #region IDataReader part II - Implementation

        /// <inheritdoc/>
        public object this[string name] => Rows[GetOrdinal(name)];
        /// <inheritdoc/>
        public object this[int i] => Rows[i];
        /// <inheritdoc/>
        public int Depth => 0;
        /// <inheritdoc/>
        public int FieldCount => ColumnMapping.Count;
        /// <inheritdoc/>
        public bool IsClosed => ReadIndex >= Rows.Count;
        /// <inheritdoc/>
        public int RecordsAffected => Rows.Count;

        Dictionary<int, string> OrdinalToName;
        Dictionary<string, int> NameToOrdinal;
        Dictionary<int, int> OrdinalToDataIndex;
        /// <inheritdoc/>
        public string GetName(int i) //check
        {
            InitDataIndexIfNeeded();
            InitOrdinalRemappingIfNeeded();

            if (i >= OrdinalToName.Count) return string.Empty;
            if (OrdinalToName.ContainsKey(i))
                return OrdinalToName[i];
            else
                return string.Empty;
        }

        /// <inheritdoc/>
        public string GetDataTypeName(int i) => GetDataTypeName(GetName(i));

        /// <inheritdoc/>
        public int GetOrdinal(string name)
        {
            InitDataIndexIfNeeded();
            InitOrdinalRemappingIfNeeded();

            //https://docs.microsoft.com/en-us/dotnet/api/system.data.idatarecord.getordinal?view=net-5.0            
            return NameToOrdinal.ContainsKey(name) ?
                NameToOrdinal[name] : NameToOrdinal.Count; //if out of range, then GetValue returns null 
        }

        /// <inheritdoc/>        
        public object GetValue(int i) => GetCurrentRow(i);  //always used, null if out of range

        /// <inheritdoc/>
        public bool IsDBNull(int i) => GetCurrentRow(i) == null; //always used, null if out of range

        /// <inheritdoc/>
        public bool NextResult()
        {
            return (ReadIndex + 1) <= Rows?.Count;
        }

        /// <inheritdoc/>
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
            if (definition == null) throw new ArgumentNullException(nameof(Definition),
                "ETLBox: TablDefinition is needed to create a proper TableData object!");
            Definition = definition;
            Rows = new List<object[]>(estimatedBatchSize);
        }

        #endregion

        #region Implementation

        IColumnMappingCollection _columnMapping;

        private void InitDataIndexIfNeeded()
        {
            if (DataIndexForColumn.Count == 0)
                CreateDefaultIndexFromColumnMapping();
        }

        private void InitOrdinalRemappingIfNeeded()
        {
            if (OrdinalToName != null) return;
            OrdinalToName = new Dictionary<int, string>();
            NameToOrdinal = new Dictionary<string, int>();
            OrdinalToDataIndex = new Dictionary<int, int>();            
            int newIndex = 0;
            foreach (var dataColName in DataIndexForColumn.Keys)
            {
                if (Definition.Columns.Any(col => col.Name == dataColName &&
                        (!col.IsIdentity || KeepIdentity)))
                {
                    NameToOrdinal.Add(dataColName, newIndex);
                    OrdinalToName.Add(newIndex, dataColName);
                    OrdinalToDataIndex.Add(newIndex, DataIndexForColumn[dataColName]);
                    newIndex++;
                }
            }
        }

        private object GetCurrentRow(int i)
        {
            int shifted = Remap(i);
            object result; 
            if (CurrentRow.Length > shifted)
                result = CurrentRow[shifted];
            else
                result = null;
            if (ColumnConverters?.Count > 0 
                && OrdinalToName.ContainsKey(i) && ColumnConverters.ContainsKey(OrdinalToName[i]))
                return ColumnConverters[OrdinalToName[i]].Invoke(result);
            else 
                return result;
        }

        int Remap(int i) => OrdinalToDataIndex?.Count > 0 ? OrdinalToDataIndex[i] : i;

        private void CreateDefaultIndexFromColumnMapping()
        {
            for (int i = 0; i < ColumnMapping.Count; i++)
                DataIndexForColumn.Add(((DataColumnMapping)ColumnMapping[i]).SourceColumn, i);
        }

        private IColumnMappingCollection CreateColumnMappingFromDefinition()
        {
            var mapping = new DataColumnMappingCollection();
            foreach (var col in Definition.Columns)
            {
                if (!col.IsIdentity || KeepIdentity)
                {
                    if (DataIndexForColumn.Count > 0)
                    {
                        if (DataIndexForColumn.ContainsKey(col.Name))
                            mapping.Add(new DataColumnMapping(col.Name, col.Name));
                    }
                    else //Default: always a complete mapping
                    {
                        mapping.Add(new DataColumnMapping(col.Name, col.Name));
                    }
                }
            }
            if (mapping.Count == 0)
                throw new ETLBoxException($"Unable to create a column mapping between the destination table {Definition.Name} and input data type." +
                    $" There were no matching columns found that could be used to write data from the input into the target." +                    
                    $" Please check if either the properties of your data type match with the column names (case-sensitive!) or provide a column mapping.");
            return mapping;
        }



        #endregion

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

        /// Disposes the internal list that holds data
        public void Dispose()
        {
            Dispose(true);
        }

        /// Disposes the internal list that holds data
        public void Close()
        {
            Dispose();
        }

        #endregion
    }
}
