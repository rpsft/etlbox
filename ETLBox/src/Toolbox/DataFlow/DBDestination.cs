using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

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
    public class DBDestination<TInput> : DataFlowTask, ITask, IDataFlowDestination<TInput>
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DF_DBDEST";
        public override string TaskName => $"Dataflow: Write Data batchwise into table {DestinationTableDefinition.Name}";
        public override void Execute() { throw new Exception("Dataflow destinations can't be started directly"); }

        /* Public properties */
        public TableDefinition DestinationTableDefinition { get; set; }
        public bool HasDestinationTableDefinition => DestinationTableDefinition != null;
        public string TableName { get; set; }
        public bool HasTableName => !String.IsNullOrWhiteSpace(TableName);
        public Func<TInput[], TInput[]> BeforeBatchWrite { get; set; }

        public ITargetBlock<TInput> TargetBlock => Buffer;

        /* Private stuff */
        int BatchSize { get; set; } = DEFAULT_BATCH_SIZE;
        const int DEFAULT_BATCH_SIZE = 100000;
        internal BatchBlock<TInput> Buffer { get; set; }
        internal ActionBlock<TInput[]> TargetAction { get; set; }
        NLog.Logger NLogger { get; set; }
        private int ThresholdCount { get; set; } = 1;
        TypeInfo TypeInfo { get; set; }
        public DBDestination()
        {
            InitObjects(DEFAULT_BATCH_SIZE);

        }

        public DBDestination(int batchSize)
        {
            BatchSize = batchSize;
            InitObjects(batchSize);
        }

        public DBDestination(TableDefinition tableDefinition)
        {
            DestinationTableDefinition = tableDefinition;
            InitObjects(DEFAULT_BATCH_SIZE);
        }

        public DBDestination(string tableName)
        {
            TableName = tableName;
            InitObjects(DEFAULT_BATCH_SIZE);
        }

        public DBDestination(IConnectionManager connectionManager, string tableName)
        {
            ConnectionManager = connectionManager;
            TableName = tableName;
            InitObjects(DEFAULT_BATCH_SIZE);
        }

        public DBDestination(string tableName, int batchSize)
        {
            TableName = tableName;
            InitObjects(batchSize);
        }

        public DBDestination(IConnectionManager connectionManager, string tableName, int batchSize)
        {
            ConnectionManager = connectionManager;
            TableName = tableName;
            InitObjects(batchSize);
        }

        public DBDestination(TableDefinition tableDefinition, int batchSize)
        {
            DestinationTableDefinition = tableDefinition;
            BatchSize = batchSize;
            InitObjects(batchSize);
        }

        public DBDestination(string name, TableDefinition tableDefinition, int batchSize)
        {
            this.TaskName = name;
            DestinationTableDefinition = tableDefinition;
            BatchSize = batchSize;
            InitObjects(batchSize);
        }

        private void InitObjects(int batchSize)
        {
            NLogger = NLog.LogManager.GetLogger("ETL");
            Buffer = new BatchBlock<TInput>(batchSize);
            TargetAction = new ActionBlock<TInput[]>(d => WriteBatch(d));
            Buffer.LinkTo(TargetAction, new DataflowLinkOptions() { PropagateCompletion = true });
            TypeInfo = new TypeInfo(typeof(TInput));
        }

        private void WriteBatch(TInput[] data)
        {
            if (!HasDestinationTableDefinition) LoadTableDefinitionFromTableName();
            if (ProgressCount == 0) NLogStart();
            if (BeforeBatchWrite != null)
                data = BeforeBatchWrite.Invoke(data);
            TableData<TInput> td = new TableData<TInput>(DestinationTableDefinition, DEFAULT_BATCH_SIZE);
            td.Rows = ConvertRows(data);
            new SqlTask(this, $"Execute Bulk insert into {DestinationTableDefinition.Name}")
            { DisableLogging = true }
            .BulkInsert(td, DestinationTableDefinition.Name);
            LogProgress(data.Length);
        }

        private void LoadTableDefinitionFromTableName()
        {
            if (HasTableName)
                DestinationTableDefinition = TableDefinition.GetDefinitionFromTableName(TableName, this.DbConnectionManager);
            else if (!HasDestinationTableDefinition && !HasTableName)
                throw new ETLBoxException("No Table definition or table name found! You must provide a table name or a table definition.");
        }


        private List<object[]> ConvertRows(TInput[] data)
        {
            List<object[]> result = new List<object[]>();
            foreach (var CurrentRow in data)
            {
                object[] rowResult;
                if (TypeInfo.IsArray)
                {
                    rowResult = CurrentRow as object[];
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

        public void Wait()
        {
            TargetAction.Completion.Wait();
            NLogFinish();
        }

        void NLogStart()
        {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void NLogFinish()
        {
            if (!DisableLogging && HasLoggingThresholdRows)
                NLogger.Info(TaskName + $" processed {ProgressCount} records in total.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void LogProgress(int rowsProcessed)
        {
            ProgressCount += rowsProcessed;
            if (!DisableLogging && HasLoggingThresholdRows && ProgressCount >= (LoggingThresholdRows * ThresholdCount))
            {
                NLogger.Info(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
                ThresholdCount++;
            }
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

        public DBDestination(TableDefinition tableDefinition) : base(tableDefinition) { }

        public DBDestination(string tableName) : base(tableName) { }

        public DBDestination(IConnectionManager connectionManager, string tableName) : base(connectionManager, tableName) { }

        public DBDestination(string tableName, int batchSize) : base(tableName, batchSize) { }

        public DBDestination(IConnectionManager connectionManager, string tableName, int batchSize) : base(connectionManager, tableName, batchSize) { }

        public DBDestination(TableDefinition tableDefinition, int batchSize) : base(tableDefinition, batchSize) { }

        public DBDestination(string name, TableDefinition tableDefinition, int batchSize) : base(name, tableDefinition, batchSize) { }
    }

}
