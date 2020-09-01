using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace ETLBox.ControlFlow
{
    /// <summary>
    /// A column in table
    /// </summary>
    public class TableColumn : IColumnMapping
    {
        private string _dataSetColumn;
        private string _sourceColumn;

        /// <summary>
        /// Name of the column
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The sql data type of the column (e.g. "INT" or "VARCHAR(30)")
        /// </summary>
        public string DataType { get; set; }

        /// <summary>
        /// Postgres only: The internal data type may differ from the defined <see cref="DataType"/>.
        /// This will return the internal used data type.
        /// </summary>
        public string InternalDataType { get; internal set; } //Postgres only

        /// <summary>
        /// True, if the column is nullable. By default a column is not nullable.
        /// </summary>
        public bool AllowNulls { get; set; }

        /// <summary>
        /// True, if the column is used as an Identity column (auto increment in MySql or serial in Postgres)
        /// Not every database supports this.
        /// </summary>
        ///
        public bool IsIdentity { get; set; }

        /// <summary>
        /// True if the column is part of the primary key
        /// </summary>
        public bool IsPrimaryKey { get; set; }

        /// <summary>
        /// Define a default value for the column.
        /// Not all databases may support this.
        /// </summary>
        public string DefaultValue { get; set; }

        /// <summary>
        /// The collation used for the column. Leave empty if you want to use the default collation.
        /// </summary>
        public string Collation { get; set; }

        /// <summary>
        /// The calculation if the column should be computed.
        /// Not all databases may support this.
        /// </summary>
        public string ComputedColumn { get; set; }

        internal bool HasComputedColumn => !String.IsNullOrWhiteSpace(ComputedColumn);

        /// <summary>
        /// The corresponding .NET data type retrieved from the sql <see cref="DataType"/>.
        /// </summary>
        public System.Type NETDataType => DataTypeConverter.GetTypeObject(DataType);

        /// <summary>
        /// Only for MySql: A comment for the column
        /// </summary>
        public string Comment { get; set; } //MySql only

        /// <summary>
        /// Only SqlServer: The seed for and identity column
        /// </summary>
        public int? IdentitySeed { get; set; } //Sql Server only

        /// <summary>
        /// Only SqlServer: The increment value for an identity column
        /// </summary>
        public int? IdentityIncrement { get; set; } //Sql Server only

        #region IColumnMapping implemtation

        /// <inheritdoc/>
        public string DataSetColumn
        {
            get { return String.IsNullOrWhiteSpace(_dataSetColumn) ? Name : _dataSetColumn; }
            set
            {
                _dataSetColumn = value;
            }
        }

        /// <inheritdoc/>
        public string SourceColumn
        {
            get { return String.IsNullOrWhiteSpace(_sourceColumn) ? Name : _sourceColumn; }
            set
            {
                _sourceColumn = value;
            }
        }

        #endregion

        #region Constructors

        public TableColumn() { }
        public TableColumn(string name, string dataType) : this()
        {
            Name = name;
            DataType = dataType;
        }

        public TableColumn(string name, string dataType, bool allowNulls) : this(name, dataType)
        {
            AllowNulls = allowNulls;
        }

        public TableColumn(string name, string dataType, bool allowNulls, bool isPrimaryKey) : this(name, dataType, allowNulls)
        {
            IsPrimaryKey = isPrimaryKey;
        }

        public TableColumn(string name, string dataType, bool allowNulls, bool isPrimaryKey, bool isIdentity) : this(name, dataType, allowNulls, isPrimaryKey)
        {
            IsIdentity = isIdentity;
        }

        #endregion

        internal static string ColumnsAsString(IEnumerable<TableColumn> columns, string prefix = "", string suffix = "") =>
            string.Join(", ", columns.Select(col => prefix + col.Name + suffix));
    }
}
