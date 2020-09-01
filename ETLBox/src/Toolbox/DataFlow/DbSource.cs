using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.Exceptions;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// A database source defines either a table or sql query that returns data from a database.
    /// Multiple database are supported. Use the corresponding connection manager that fits to your database.
    /// </summary>
    /// <typeparam name="TOutput">Type of outgoing data.</typeparam>
    /// <example>
    /// <code>
    /// SqlConnectionManager connMan = new SqlConnectionManager("Data Source=localhost");
    /// DbSource&lt;MyRow&gt; source = new DbSource&lt;MyRow&gt;(connMan, "dbo.table");
    /// </code>
    /// </example>
    public class DbSource<TOutput> : DataFlowExecutableSource<TOutput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName => $"Read data from {SourceDescription}";

        /// <summary>
        /// Pass a table definition that describe the source table.
        /// Column names can be read from the table definition (if a table name is given)
        /// or extracted from the sql query. If a TableDefinition is present, this will always be used to determine the columns names.
        /// </summary>
        ///
        public TableDefinition SourceTableDefinition { get; set; }

        /// <summary>
        /// By default, column names are read from the table defition, extracted from the database or parsed from the sql query.
        /// The column name is used to map the data from the database source to the right property in the object used for the data flow.
        /// If you enter your own column name list, this will override any column names that exist in the source.
        /// </summary>
        public List<string> ColumnNames { get; set; }

        /// <summary>
        /// The name of the database table to read data from.
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// A custom sql query to extract the data from the source.
        /// </summary>
        public string Sql { get; set; }

        #endregion

        #region Connection Manager

        /// <summary>
        /// The connection manager used to connect to the database - use the right connection manager for your database type.
        /// </summary>
        public virtual IConnectionManager ConnectionManager { get; set; }

        internal virtual IConnectionManager DbConnectionManager
        {
            get
            {
                if (ConnectionManager == null)
                    return (IConnectionManager)ControlFlow.ControlFlow.DefaultDbConnection;
                else
                    return (IConnectionManager)ConnectionManager;
            }
        }

        private string QB => DbConnectionManager.QB;
        private string QE => DbConnectionManager.QE;
        private ConnectionManagerType ConnectionType => this.DbConnectionManager.ConnectionManagerType;
        #endregion

        #region Constructors

        public DbSource()
        {
            TypeInfo = new DBTypeInfo(typeof(TOutput));
        }

        /// <param name="tableName">Sets the <see cref="TableName" /></param>
        public DbSource(string tableName) : this()
        {
            TableName = tableName;
        }

        /// <param name="connectionManager">Sets the <see cref="ConnectionManager" /></param>
        public DbSource(IConnectionManager connectionManager) : this()
        {
            ConnectionManager = connectionManager;
        }

        /// <param name="connectionManager">Sets the <see cref="ConnectionManager" /></param>
        /// <param name="tableName">Sets the <see cref="TableName" /></param>
        public DbSource(IConnectionManager connectionManager, string tableName) : this(tableName)
        {
            ConnectionManager = connectionManager;
        }

        #endregion

        #region Implement abstract methods

        protected override void OnExecutionDoSynchronousWork()
        {
            NLogStartOnce();
        }

        protected override void OnExecutionDoAsyncWork()
        {
            ReadAllRecords();
            Buffer.Complete();
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        #endregion

        #region Implementation

        string SqlForRead
        {
            get
            {
                if (HasSql)
                    return Sql;
                else
                {
                    if (!HasSourceTableDefinition)
                        LoadTableDefinition();
                    var TN = new ObjectNameDescriptor(SourceTableDefinition.Name, QB, QE);
                    return $@"SELECT {TableColumn.ColumnsAsString(SourceTableDefinition.Columns, QB, QE)} FROM {TN.QuotatedFullName}";
                }

            }
        }

        List<string> ColumnNamesEvaluated
        {
            get
            {
                if (ColumnNames?.Count > 0)
                    return ColumnNames;
                else if (HasSourceTableDefinition)
                    return SourceTableDefinition?.Columns?.Select(col => col.Name).ToList();
                else
                    return ParseColumnNamesFromQuery();
            }
        }
        List<string> ParseColumnNamesFromQuery()
        {
            var result = SqlParser.ParseColumnNames(QB != string.Empty ? SqlForRead.Replace(QB, "").Replace(QE, "") : SqlForRead);
            if (TypeInfo.IsArray && result?.Count == 0) throw new ETLBoxException("Could not parse column names from Sql Query! Please pass a valid TableDefinition to the " +
                " property SourceTableDefinition with at least a name for each column that you want to use in the source."
                );
            return result;
        }

        bool HasSourceTableDefinition => SourceTableDefinition != null;
        bool HasTableName => !String.IsNullOrWhiteSpace(TableName);
        bool HasSql => !String.IsNullOrWhiteSpace(Sql);
        DBTypeInfo TypeInfo;
        string SourceDescription
        {
            get
            {
                if (HasSourceTableDefinition)
                    return $"table {SourceTableDefinition.Name}";
                if (HasTableName)
                    return $"table {TableName}";
                else
                    return "custom sql";
            }
        }

        private void ReadAllRecords()
        {
            SqlTask sqlT = CreateSqlTask(SqlForRead);
            DefineActions(sqlT, ColumnNamesEvaluated);
            sqlT.ExecuteReader();
            CleanupSqlTask(sqlT);
        }

        private void LoadTableDefinition()
        {
            if (HasTableName)
                SourceTableDefinition = TableDefinition.FromTableName(this.DbConnectionManager, TableName);
            else if (!HasSourceTableDefinition && !HasTableName)
                throw new ETLBoxException("No Table definition or table name found! You must provide a table name or a table definition.");
        }

        private SqlTask CreateSqlTask(string sql)
        {
            var sqlT = new SqlTask(sql)
            {
                DisableLogging = true,
                ConnectionManager = this.ConnectionManager
            };
            sqlT.CopyLogTaskProperties(this);
            sqlT.Actions = new List<Action<object>>();
            return sqlT;
        }

        TOutput _row;
        private void DefineActions(SqlTask sqlT, List<string> columnNames)
        {
            if (columnNames?.Count == 0 && ( TypeInfo.IsArray || TypeInfo.PropertyNames.Count == 0) )
                throw new ETLBoxException("The column names can't be automatically retrieved - please provide a TableDefinition with names for the columns in the source.");
            _row = default(TOutput);
            if (TypeInfo.IsArray)
            {
                sqlT.BeforeRowReadAction = () =>
                    _row = (TOutput)Activator.CreateInstance(typeof(TOutput), new object[] { columnNames.Count });
                int index = 0;
                foreach (var colName in columnNames)
                    index = SetupArrayFillAction(sqlT, index);
            }
            else
            {
                if (columnNames?.Count == 0) columnNames = TypeInfo.PropertyNames;
                foreach (var colName in columnNames)
                {
                    if (TypeInfo.HasPropertyOrColumnMapping(colName))
                        SetupObjectFillAction(sqlT, colName);
                    else if (TypeInfo.IsDynamic)
                        SetupDynamicObjectFillAction(sqlT, colName);
                    else
                        sqlT.Actions.Add(col => { });
                }
                sqlT.BeforeRowReadAction = () => _row = (TOutput)Activator.CreateInstance(typeof(TOutput));
            }
            sqlT.AfterRowReadAction = () =>
            {
                if (_row != null)
                {
                    LogProgress();
                    DbConnectionManager.CheckLicenseOrThrow(ProgressCount);
                    if (!Buffer.SendAsync(_row).Result)
                        throw new ETLBoxException("Buffer already completed or faulted!", this.Exception);
                }
            };
        }

        private int SetupArrayFillAction(SqlTask sqlT, int index)
        {
            int currentIndexAvoidingClosure = index;
            sqlT.Actions.Add(col =>
            {
                try
                {
                    if (_row != null)
                    {
                        var ar = _row as System.Array;
                        var con = Convert.ChangeType(col, typeof(TOutput).GetElementType());
                        ar.SetValue(con, currentIndexAvoidingClosure);
                    }
                }
                catch (Exception e)
                {
                    ThrowOrRedirectError(e, ErrorSource.ConvertErrorData<TOutput>(_row));
                    _row = default;
                }
            });
            index++;
            return index;
        }

        private void SetupObjectFillAction(SqlTask sqlT, string colName)
        {
            sqlT.Actions.Add(colValue =>
            {
                try
                {
                    if (_row != null)
                    {
                        var propInfo = TypeInfo.GetInfoByPropertyNameOrColumnMapping(colName);
                        Object con = null;
                        if (colValue != null)
                        {
                            if (TypeInfo.UnderlyingPropType[propInfo].IsEnum)
                                con = colValue;
                            else
                                con = Convert.ChangeType(colValue, TypeInfo.UnderlyingPropType[propInfo]);
                        }

                        propInfo.TrySetValue(_row, con, TypeInfo.UnderlyingPropType[propInfo]);
                    }
                }
                catch (Exception e)
                {
                    ThrowOrRedirectError(e, ErrorSource.ConvertErrorData<TOutput>(_row));
                    _row = default;
                }
            });
        }

        private void SetupDynamicObjectFillAction(SqlTask sqlT, string colName)
        {
            sqlT.Actions.Add(colValue =>
            {
                try
                {
                    if (_row != null)
                    {
                        dynamic r = _row as ExpandoObject;
                        ((IDictionary<String, Object>)r).Add(colName, colValue);
                    }
                }
                catch (Exception e)
                {
                    ThrowOrRedirectError(e, ErrorSource.ConvertErrorData<TOutput>(_row));
                    _row = default;
                }
            });
        }

        void CleanupSqlTask(SqlTask sqlT)
        {
            sqlT.Actions = null;
        }

        #endregion
    }

    /// <inheritdoc/>
    public class DbSource : DbSource<ExpandoObject>
    {
        public DbSource() : base() { }
        public DbSource(string tableName) : base(tableName) { }
        public DbSource(IConnectionManager connectionManager) : base(connectionManager) { }
        public DbSource(IConnectionManager connectionManager, string tableName) : base(connectionManager, tableName) { }
    }
}
