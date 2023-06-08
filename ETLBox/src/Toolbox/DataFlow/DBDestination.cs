using System.Linq;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// A database destination represents a table where data from the flow is inserted.
    /// Inserts are done in batches (using Bulk insert or an equivalent).
    /// </summary>
    /// <see cref="DbDestination"/>
    /// <typeparam name="TInput">Data type for input, preferably representing the destination table.</typeparam>
    [PublicAPI]
    public class DbDestination<TInput> : DataFlowBatchDestination<TInput>
    {
        /* ITask Interface */
        public override string TaskName =>
            $"Write data into table {DestinationTableDefinition?.Name ?? TableName}";

        /* Public properties */
        /// <summary>
        /// If you don't want ETLBox to dynamically read the destination table definition from the database,
        /// you can provide your own table definition.
        /// </summary>
        public TableDefinition DestinationTableDefinition { get; set; }

        /// <summary>
        /// Name of the target table that receives the data from the data flow.
        /// </summary>
        public string TableName { get; set; }

        /* Private stuff */
        private TypeInfo TypeInfo { get; set; }
        private bool HasDestinationTableDefinition => DestinationTableDefinition != null;
        private bool HasTableName => !string.IsNullOrWhiteSpace(TableName);
        private TableData<TInput> TableData { get; set; }
        private int WereDynamicColumnsAdded { get; set; }

        public IConnectionManager BulkInsertConnectionManager { get; set; }

        public DbDestination()
        {
            BatchSize = DEFAULT_BATCH_SIZE;
        }

        public DbDestination(int batchSize)
        {
            BatchSize = batchSize;
        }

        public DbDestination(string tableName)
            : this()
        {
            TableName = tableName;
        }

        public DbDestination(IConnectionManager connectionManager, string tableName)
            : this(tableName)
        {
            base.ConnectionManager = connectionManager;
        }

        public DbDestination(string tableName, int batchSize)
            : this(batchSize)
        {
            TableName = tableName;
        }

        public DbDestination(IConnectionManager connectionManager, string tableName, int batchSize)
            : this(tableName, batchSize)
        {
            base.ConnectionManager = connectionManager;
        }

        protected override void InitObjects(int batchSize)
        {
            base.InitObjects(batchSize);
            TypeInfo = new TypeInfo(typeof(TInput)).GatherTypeInfo();
        }

        private void LoadTableDefinitionFromTableName()
        {
            if (HasTableName)
                DestinationTableDefinition = TableDefinition.GetDefinitionFromTableName(
                    DbConnectionManager,
                    TableName
                );
            else if (!HasDestinationTableDefinition && !HasTableName)
                throw new ETLBoxException(
                    "No Table definition or table name found! You must provide a table name or a table definition."
                );
        }

        protected override void PrepareWrite()
        {
            if (!HasDestinationTableDefinition)
                LoadTableDefinitionFromTableName();
            BulkInsertConnectionManager = DbConnectionManager.CloneIfAllowed();
            BulkInsertConnectionManager.IsInBulkInsert = true;
            BulkInsertConnectionManager.PrepareBulkInsert(DestinationTableDefinition.Name);
            TableData = new TableData<TInput>(DestinationTableDefinition, BatchSize);
        }

        protected override void TryBulkInsertData(TInput[] data)
        {
            TryAddDynamicColumnsToTableDef(data);
            try
            {
                TableData.ClearData();
                ConvertAndAddRows(data);
                var sql = new SqlTask(this, "Execute Bulk insert")
                {
                    DisableLogging = true,
                    ConnectionManager = BulkInsertConnectionManager
                };
                sql.BulkInsert(TableData, DestinationTableDefinition.Name);
            }
            catch (Exception e)
            {
                if (!ErrorHandler.HasErrorBuffer)
                    throw;
                ErrorHandler.Send(e, ErrorHandler.ConvertErrorData(data));
            }
        }

        protected override void FinishWrite()
        {
            TableData?.Close();
            if (BulkInsertConnectionManager != null)
            {
                BulkInsertConnectionManager.IsInBulkInsert = false;
                BulkInsertConnectionManager.CleanUpBulkInsert(DestinationTableDefinition?.Name);
                BulkInsertConnectionManager.CloseIfAllowed();
            }
        }

        private void TryAddDynamicColumnsToTableDef(TInput[] data)
        {
            if (!TypeInfo.IsDynamic || data.Length <= 0)
            {
                return;
            }

            foreach (
                var dynamicObject in data.Select(x => x as IDictionary<string, object>)
                    .Where(x => x != null)
            )
            {
                foreach (var column in dynamicObject)
                {
                    int newPropIndex = TableData.DynamicColumnNames.Count;
                    if (!TableData.DynamicColumnNames.ContainsKey(column.Key))
                        TableData.DynamicColumnNames.Add(column.Key, newPropIndex);
                }
            }
        }

        private void ConvertAndAddRows(TInput[] data)
        {
            foreach (var currentRow in data)
            {
                if (currentRow == null)
                    continue;

                TableData.Rows.Add(
                    TypeInfo.IsArray
                        ? currentRow as object[]
                        : TypeInfo.IsDynamic
                            ? ConvertDynamicRow(currentRow as IDictionary<string, object>)
                            : ConvertObjectRow(currentRow)
                );
            }
        }

        private object[] ConvertObjectRow(TInput currentRow)
        {
            var rowResult = new object[TypeInfo.PropertyLength];
            int index = 0;
            foreach (PropertyInfo propInfo in TypeInfo.Properties)
            {
                rowResult[index] = propInfo.GetValue(currentRow);
                index++;
            }

            return rowResult;
        }

        private object[] ConvertDynamicRow(IDictionary<string, object> propertyValues)
        {
            var rowResult = new object[TableData.DynamicColumnNames.Count];
            foreach (var prop in propertyValues)
            {
                int columnIndex = TableData.DynamicColumnNames[prop.Key];
                rowResult[columnIndex] = prop.Value;
            }

            return rowResult;
        }
    }

    /// <summary>
    /// A database destination represents a table where data from the flow is inserted.
    /// Inserts are done in batches (using Bulk insert or an equivalent).
    /// The DbDestination uses the dynamic ExpandoObject as input type.
    /// If you need other data types, use the generic DbDestination instead.
    /// </summary>
    /// <see cref="DbDestination{TInput}"/>
    [PublicAPI]
    public class DbDestination : DbDestination<ExpandoObject>
    {
        public DbDestination() { }

        public DbDestination(int batchSize)
            : base(batchSize) { }

        public DbDestination(string tableName)
            : base(tableName) { }

        public DbDestination(IConnectionManager connectionManager, string tableName)
            : base(connectionManager, tableName) { }

        public DbDestination(string tableName, int batchSize)
            : base(tableName, batchSize) { }

        public DbDestination(IConnectionManager connectionManager, string tableName, int batchSize)
            : base(connectionManager, tableName, batchSize) { }
    }
}
