using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// A database destination defines a table where data from the flow is inserted. Inserts are done in batches (using Bulk insert).
    /// </summary>
    /// <see cref="DBDestination"/>
    /// <typeparam name="TInput">Type of data input.</typeparam>
    /// <example>
    /// <code>
    /// DBDestination&lt;MyRow&gt; dest = new DBDestination&lt;MyRow&gt;("dbo.table");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class DBDestination<TInput> : DataFlowBatchDestination<TInput>, ITask, IDataFlowDestination<TInput>
    {
        /* ITask Interface */
        public override string TaskName => $"Write data into table {DestinationTableDefinition?.Name ?? TableName}";
        /* Public properties */
        public TableDefinition DestinationTableDefinition { get; set; }
        public bool HasDestinationTableDefinition => DestinationTableDefinition != null;
        public string TableName { get; set; }
        public bool HasTableName => !String.IsNullOrWhiteSpace(TableName);


        internal const int DEFAULT_BATCH_SIZE = 1000;


        public DBDestination()
        {
            BatchSize = DEFAULT_BATCH_SIZE;
        }

        public DBDestination(int batchSize)
        {
            BatchSize = batchSize;
        }

        public DBDestination(string tableName) : this()
        {
            TableName = tableName;
        }

        public DBDestination(IConnectionManager connectionManager, string tableName) : this(tableName)
        {
            ConnectionManager = connectionManager;
        }

        public DBDestination(string tableName, int batchSize) : this(batchSize)
        {
            TableName = tableName;
        }

        public DBDestination(IConnectionManager connectionManager, string tableName, int batchSize) : this(tableName, batchSize)
        {
            ConnectionManager = connectionManager;
        }

        internal override void InitObjects(int batchSize)
        {
            base.InitObjects(batchSize);
        }

        internal override void WriteBatch(ref TInput[] data)
        {
            if (!HasDestinationTableDefinition) LoadTableDefinitionFromTableName();

            base.WriteBatch(ref data);

            TryBulkInsertData(ref data);

            LogProgressBatch(data.Length);
        }

        private void LoadTableDefinitionFromTableName()
        {
            if (HasTableName)
                DestinationTableDefinition = TableDefinition.GetDefinitionFromTableName(TableName, this.DbConnectionManager);
            else if (!HasDestinationTableDefinition && !HasTableName)
                throw new ETLBoxException("No Table definition or table name found! You must provide a table name or a table definition.");
        }

        private void TryBulkInsertData(ref TInput[] data)
        {
            TableData<TInput> td = CreateTableDataObject(ref data);
            try
            {
                new SqlTask(this, $"Execute Bulk insert")
                {
                    DisableLogging = true
                }
                .BulkInsert(td, DestinationTableDefinition.Name);
            }
            catch (Exception e)
            {
                if (!ErrorHandler.HasErrorBuffer) throw e;
                ErrorHandler.Post(e, ErrorHandler.ConvertErrorData<TInput[]>(data));
            }
        }

        private TableData<TInput> CreateTableDataObject(ref TInput[] data)
        {
            TableData<TInput> td = new TableData<TInput>(DestinationTableDefinition, DEFAULT_BATCH_SIZE);
            td.Rows = ConvertRows(ref data);
            if (TypeInfo.IsDynamic && data.Length > 0)
                foreach (var column in (IDictionary<string, object>)data[0])
                    td.DynamicColumnNames.Add(column.Key);
            return td;
        }

        private List<object[]> ConvertRows(ref TInput[] data)
        {
            List<object[]> result = new List<object[]>();
            foreach (var CurrentRow in data)
            {
                if (CurrentRow == null) continue;
                object[] rowResult;
                if (TypeInfo.IsArray)
                {
                    rowResult = CurrentRow as object[];
                }
                else if (TypeInfo.IsDynamic)
                {
                    IDictionary<string, object> propertyValues = (IDictionary<string, object>)CurrentRow;
                    rowResult = new object[propertyValues.Count];
                    int index = 0;
                    foreach (var prop in propertyValues)
                    {
                        rowResult[index] = prop.Value;
                        index++;
                    }
                }
                else
                {
                    rowResult = new object[TypeInfo.PropertyLength];
                    int index = 0;
                    foreach (PropertyInfo propInfo in TypeInfo.Properties)
                    {
                        rowResult[index] = propInfo.GetValue(CurrentRow);
                        index++;
                    }
                }
                result.Add(rowResult);
            }
            return result;
        }
    }

    /// <summary>
    /// A database destination defines a table where data from the flow is inserted. Inserts are done in batches (using Bulk insert).
    /// The DBDestination access a string array as input type. If you need other data types, use the generic DBDestination instead.
    /// </summary>
    /// <see cref="DBDestination{TInput}"/>
    /// <example>
    /// <code>
    /// //Non generic DBDestination works with string[] as input
    /// //use DBDestination&lt;TInput&gt; for generic usage!
    /// DBDestination dest = new DBDestination("dbo.table");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class DBDestination : DBDestination<string[]>
    {
        public DBDestination() : base() { }

        public DBDestination(int batchSize) : base(batchSize) { }

        public DBDestination(string tableName) : base(tableName) { }

        public DBDestination(IConnectionManager connectionManager, string tableName) : base(connectionManager, tableName) { }

        public DBDestination(string tableName, int batchSize) : base(tableName, batchSize) { }

        public DBDestination(IConnectionManager connectionManager, string tableName, int batchSize) : base(connectionManager, tableName, batchSize) { }
    }

}
