using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System.Linq;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// A database source defines either a table or sql query that returns data from a database. While reading the result set or the table, data is asnychronously posted
    /// into the targets.
    /// </summary>
    /// <typeparam name="TOutput">Type of data output.</typeparam>
    /// <example>
    /// <code>
    /// DbSource&lt;MyRow&gt; source = new DbSource&lt;MyRow&gt;("dbo.table");
    /// source.LinkTo(dest); //Transformation or Destination
    /// source.Execute(); //Start the data flow
    /// </code>
    /// </example>
    [PublicAPI]
    public class DbSource<TOutput> : DataFlowSource<TOutput>, IDataFlowSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskName => $"Read data from {SourceDescription}";

        /* Public Properties */
        public TableDefinition SourceTableDefinition { get; set; }
        public List<string> ColumnNames { get; set; }
        public string TableName { get; set; }
        public string Sql { get; set; }

        public string SqlForRead
        {
            get
            {
                if (HasSql)
                    return Sql;
                else
                {
                    if (!HasSourceTableDefinition)
                        LoadTableDefinition();
                    var tn = new ObjectNameDescriptor(SourceTableDefinition.Name, QB, QE);
                    return $@"SELECT {SourceTableDefinition.Columns.AsString("", QB, QE)} FROM {tn.QuotatedFullName}";
                }
            }
        }

        public List<string> ColumnNamesEvaluated
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

        private bool HasSourceTableDefinition => SourceTableDefinition != null;
        private bool HasTableName => !string.IsNullOrWhiteSpace(TableName);
        private bool HasSql => !string.IsNullOrWhiteSpace(Sql);
        private DBTypeInfo TypeInfo { get; set; }

        private string SourceDescription
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

        public DbSource()
        {
            TypeInfo = new DBTypeInfo(typeof(TOutput));
        }

        public DbSource(string tableName)
            : this()
        {
            TableName = tableName;
        }

        public DbSource(IConnectionManager connectionManager)
            : this()
        {
            ConnectionManager = connectionManager;
        }

        public DbSource(IConnectionManager connectionManager, string tableName)
            : this(tableName)
        {
            ConnectionManager = connectionManager;
        }

        private List<string> ParseColumnNamesFromQuery()
        {
            var result = SqlParser.ParseColumnNames(
                QB != string.Empty ? SqlForRead.Replace(QB, "").Replace(QE, "") : SqlForRead
            );
            if (TypeInfo.IsArray && result?.Count == 0)
                throw new ETLBoxException(
                    "Could not parse column names from Sql Query! Please pass a valid TableDefinition to the "
                        + " property SourceTableDefinition with at least a name for each column that you want to use in the source."
                );
            return result;
        }

        public override void Execute()
        {
            NLogStart();
            try
            {
                ReadAll();
                Buffer.Complete();
            }
            catch (Exception e)
            {
                ((IDataflowBlock)Buffer).Fault(e);
                throw;
            }
            Buffer.Complete();
            NLogFinish();
        }

        private void ReadAll()
        {
            SqlTask sqlT = CreateSqlTask(SqlForRead);
            DefineActions(sqlT, ColumnNamesEvaluated);
            sqlT.ExecuteReader();
            CleanupSqlTask(sqlT);
        }

        private void LoadTableDefinition()
        {
            if (HasTableName)
                SourceTableDefinition = TableDefinition.GetDefinitionFromTableName(
                    this.DbConnectionManager,
                    TableName
                );
            else if (!HasSourceTableDefinition && !HasTableName)
                throw new ETLBoxException(
                    "No Table definition or table name found! You must provide a table name or a table definition."
                );
        }

        private SqlTask CreateSqlTask(string sql) =>
            new(this, sql) { DisableLogging = true, Actions = new List<Action<object>>() };

        private TOutput _row;

        internal void DefineActions(SqlTask sqlT, List<string> columnNames)
        {
            _row = default;
            if (TypeInfo.IsArray)
            {
                sqlT.BeforeRowReadAction = () =>
                    _row = (TOutput)Activator.CreateInstance(typeof(TOutput), columnNames.Count);
                int index = 0;
                foreach (var _ in columnNames)
                    index = SetupArrayFillAction(sqlT, index);
            }
            else
            {
                if (columnNames?.Count == 0)
                    columnNames = TypeInfo.PropertyNames;
                SetupNonArrayFillAction(sqlT, columnNames);
                sqlT.BeforeRowReadAction = () =>
                    _row = (TOutput)Activator.CreateInstance(typeof(TOutput));
            }
            sqlT.AfterRowReadAction = () =>
            {
                if (_row != null)
                {
                    LogProgress();
                    Buffer.SendAsync(_row).Wait();
                }
            };
        }

        private void SetupNonArrayFillAction(SqlTask sqlT, List<string> columnNames)
        {
            foreach (var colName in columnNames)
            {
                if (TypeInfo.HasPropertyOrColumnMapping(colName))
                    SetupObjectFillAction(sqlT, colName);
                else if (TypeInfo.IsDynamic)
                    SetupDynamicObjectFillAction(sqlT, colName);
                else
                    sqlT.Actions.Add(_ => { });
            }
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
                        var ar = _row as Array;
                        var con = Convert.ChangeType(col, typeof(TOutput).GetElementType()!);
                        ar!.SetValue(con, currentIndexAvoidingClosure);
                    }
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer)
                        throw;
                    _row = default;
                    ErrorHandler.Send(e, ErrorHandler.ConvertErrorData(_row));
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
                        var con =
                            colValue != null
                                ? TypeInfo.UnderlyingPropType[propInfo].IsEnum
                                    ? colValue
                                    : Convert.ChangeType(
                                        colValue,
                                        TypeInfo.UnderlyingPropType[propInfo]
                                    )
                                : null;

                        propInfo.TrySetValue(_row, con, TypeInfo.UnderlyingPropType[propInfo]);
                    }
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer)
                        throw;
                    _row = default;
                    ErrorHandler.Send(e, ErrorHandler.ConvertErrorData(_row));
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
                        IDictionary<string, object> r = _row as ExpandoObject;
                        r!.Add(colName, colValue);
                    }
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer)
                        throw;
                    _row = default;
                    ErrorHandler.Send(e, ErrorHandler.ConvertErrorData(_row));
                }
            });
        }

        private void CleanupSqlTask(SqlTask sqlT)
        {
            sqlT.Actions = null;
        }
    }

    /// <summary>
    /// A database source defines either a table or sql query that returns data from a database. While reading the result set or the table, data is asnychronously posted
    /// into the targets. The non generic version of the DbSource uses a dynamic object that contains the data.
    /// </summary>
    /// <see cref="DbSource{TOutput}"/>
    /// <example>
    /// <code>
    /// DbSource source = new DbSource("dbo.table");
    /// source.LinkTo(dest); //Transformation or Destination
    /// source.Execute(); //Start the data flow
    /// </code>
    /// </example>
    [PublicAPI]
    public class DbSource : DbSource<ExpandoObject>
    {
        public DbSource() { }

        public DbSource(string tableName)
            : base(tableName) { }

        public DbSource(IConnectionManager connectionManager)
            : base(connectionManager) { }

        public DbSource(IConnectionManager connectionManager, string tableName)
            : base(connectionManager, tableName) { }
    }
}
