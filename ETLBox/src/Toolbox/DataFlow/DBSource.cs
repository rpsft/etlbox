using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using TSQL;
using TSQL.Statements;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// A database source defines either a table or sql query that returns data from a database. While reading the result set or the table, data is asnychronously posted
    /// into the targets.
    /// </summary>
    /// <typeparam name="TOutput">Type of data output.</typeparam>
    /// <example>
    /// <code>
    /// DBSource&lt;MyRow&gt; source = new DBSource&lt;MyRow&gt;("dbo.table");
    /// source.LinkTo(dest); //Transformation or Destination
    /// source.Execute(); //Start the data flow
    /// </code>
    /// </example>
    public class DBSource<TOutput> : DataFlowSource<TOutput>, ITask, IDataFlowSource<TOutput>
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
                    var TN = new ObjectNameDescriptor(SourceTableDefinition.Name, ConnectionType);
                    return $@"SELECT {SourceTableDefinition.Columns.AsString("", QB, QE)} FROM {TN.QuotatedFullName}";
                }

            }
        }

        public List<string> ColumnNamesEvaluated
        {
            get
            {
                if (HasColumnNames)
                    return ColumnNames;
                else if (HasSourceTableDefinition)
                    return SourceTableDefinition?.Columns?.Select(col => col.Name).ToList();
                else
                    return SqlParser.ParseColumnNames(QB != string.Empty ? SqlForRead.Replace(QB, "").Replace(QE, "") : SqlForRead);
            }
        }

        bool HasSourceTableDefinition => SourceTableDefinition != null;
        bool HasColumnNames => ColumnNames != null && ColumnNames?.Count > 0;
        bool HasTableName => !String.IsNullOrWhiteSpace(TableName);
        bool HasSql => !String.IsNullOrWhiteSpace(Sql);
        DBTypeInfo TypeInfo { get; set; }
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

        public DBSource()
        {
            TypeInfo = new DBTypeInfo(typeof(TOutput));
        }

        public DBSource(string tableName) : this()
        {
            TableName = tableName;
        }

        public DBSource(IConnectionManager connectionManager) : this()
        {
            ConnectionManager = connectionManager;
        }

        public DBSource(IConnectionManager connectionManager, string tableName) : this(tableName)
        {
            ConnectionManager = connectionManager;
        }

        public override void Execute()
        {
            NLogStart();
            ReadAll();
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
                SourceTableDefinition = TableDefinition.GetDefinitionFromTableName(this.DbConnectionManager, TableName);
            else if (!HasSourceTableDefinition && !HasTableName)
                throw new ETLBoxException("No Table definition or table name found! You must provide a table name or a table definition.");
        }

        SqlTask CreateSqlTask(string sql)
        {
            var sqlT = new SqlTask(this, sql)
            {
                DisableLogging = true,
            };
            sqlT.Actions = new List<Action<object>>();
            return sqlT;
        }

        TOutput _row;
        internal void DefineActions(SqlTask sqlT, List<string> columnNames)
        {
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
                    Buffer.SendAsync(_row).Wait();
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
                    if (!ErrorHandler.HasErrorBuffer) throw e;
                    _row = default(TOutput);
                    ErrorHandler.Send(e, ErrorHandler.ConvertErrorData<TOutput>(_row));
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
                        var con = colValue != null ? Convert.ChangeType(colValue, TypeInfo.UnderlyingPropType[propInfo]) : colValue;
                        propInfo.TrySetValue(_row, con);
                    }
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer) throw e;
                    _row = default(TOutput);
                    ErrorHandler.Send(e, ErrorHandler.ConvertErrorData<TOutput>(_row));
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
                    if (!ErrorHandler.HasErrorBuffer) throw e;
                    _row = default(TOutput);
                    ErrorHandler.Send(e, ErrorHandler.ConvertErrorData<TOutput>(_row));
                }
            });
        }

        void CleanupSqlTask(SqlTask sqlT)
        {
            sqlT.Actions = null;
        }


    }

    /// <summary>
    /// A database source defines either a table or sql query that returns data from a database. While reading the result set or the table, data is asnychronously posted
    /// into the targets. The non generic version of the DBSource creates a string array that contains the data.
    /// </summary>
    /// <see cref="DBSource{TOutput}"/>
    /// <example>
    /// <code>
    /// //Non generic DBSource works with string[] as output
    /// //use DBSource&lt;TOutput&gt; for generic usage!
    /// DBSource source = new DBSource("dbo.table");
    /// source.LinkTo(dest); //Transformation or Destination
    /// source.Execute(); //Start the data flow
    /// </code>
    /// </example>
    public class DBSource : DBSource<string[]>
    {
        public DBSource() : base() { }
        public DBSource(string tableName) : base(tableName) { }
        public DBSource(IConnectionManager connectionManager) : base(connectionManager) { }
        public DBSource(IConnectionManager connectionManager, string tableName) : base(connectionManager, tableName) { }
    }
}
