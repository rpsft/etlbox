using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Linq;
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
        public override string TaskName => $"Dataflow: Read DB data from {SourceDescription}";

        /* Public Properties */
        public TableDefinition SourceTableDefinition { get; set; }
        public bool HasSourceTableDefinition => SourceTableDefinition != null;
        public List<string> ColumnNames { get; set; }
        public bool HasColumnNames => ColumnNames != null && ColumnNames?.Count > 0;
        public string TableName { get; set; }
        public bool HasTableName => !String.IsNullOrWhiteSpace(TableName);
        public string Sql { get; set; }
        public bool HasSql => !String.IsNullOrWhiteSpace(Sql);
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
                    var TN = new TableNameDescriptor(SourceTableDefinition.Name, ConnectionType);
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
                    return ParseColumnNamesFromSql();
            }
        }

        public string SourceDescription
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

        public void Execute() => PostAll();
        public void PostAll()
        {
            NLogStart();
            ReadAll();
            Buffer.Complete();
            NLogFinish();
        }

        private void ReadAll()
        {
            new SqlTask(this, SqlForRead)
            {
                DisableLogging = true,
                DisableExtension = true,
            }.Query<TOutput>(row =>
            {
                LogProgress(1);
                Buffer.Post(row);
            }, ColumnNamesEvaluated);
        }

        private List<string> ParseColumnNamesFromSql()
        {
            var sql = QB != string.Empty ? SqlForRead.Replace(QB, "").Replace(QE, "") : SqlForRead;
            var statement = TSQLStatementReader.ParseStatements(sql).FirstOrDefault() as TSQLSelectStatement;

            List<string> columNames = statement?.Select
                                                .Tokens
                                                .Where(token => token.Type == TSQL.Tokens.TSQLTokenType.Identifier)
                                                .Select(token => token.AsIdentifier.Name)
                                                .ToList();
            return columNames;
        }

        private void LoadTableDefinition()
        {
            if (HasTableName)
                SourceTableDefinition = TableDefinition.GetDefinitionFromTableName(TableName, this.ConnectionManager);
            else if (!HasSourceTableDefinition && !HasTableName)
                throw new ETLBoxException("No Table definition or table name found! You must provide a table name or a table definition.");
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
