using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Inserts, updates and (optionally) deletes data in db target.
    /// </summary>
    /// <typeparam name="TInput">Type of input data.</typeparam>
    /// <example>
    /// <code>
    /// </code>
    /// </example>
    public class DbMerge<TInput> : DataFlowTransformation<TInput, TInput>, ITask, IDataFlowTransformation<TInput, TInput> where TInput : IMergeableRow, new()
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = "Insert, Upsert or delete in destination";

        public async Task ExecuteAsync() => await OutputSource.ExecuteAsync();
        public void Execute() => OutputSource.Execute();

        /* Public Properties */
        public override ISourceBlock<TInput> SourceBlock => OutputSource.SourceBlock;
        public override ITargetBlock<TInput> TargetBlock => Lookup.TargetBlock;
        public bool DisableDeletion { get; set; }
        public TableDefinition DestinationTableDefinition { get; set; }
        public string TableName { get; set; }
        public List<TInput> DeltaTable { get; set; } = new List<TInput>();
        public bool UseTruncateMethod
        {
            get
            {
                if (TypeInfo?.IdColumnNames == null || TypeInfo?.IdColumnNames?.Count == 0) return true;
                return _useTruncateMethod;
            }
            set
            {
                _useTruncateMethod = value;
            }
        }

        /* Private stuff */
        bool _useTruncateMethod;
        ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, ConnectionType);
        LookupTransformation<TInput, TInput> Lookup { get; set; }
        DbSource<TInput> DestinationTableAsSource { get; set; }
        DbDestination<TInput> DestinationTable { get; set; }
        List<TInput> InputData => Lookup.LookupData;
        CustomSource<TInput> OutputSource { get; set; }
        bool WasTruncationExecuted { get; set; }
        DBMergeTypeInfo TypeInfo { get; set; }

        public DbMerge(string tableName)
        {
            TableName = tableName;
            DestinationTableAsSource = new DbSource<TInput>(TableName);
            DestinationTable = new DbDestination<TInput>(TableName);
            Init();
        }

        public DbMerge(IConnectionManager connectionManager, string tableName) : this(tableName)
        {
            TableName = tableName;
            ConnectionManager = connectionManager;
            DestinationTableAsSource = new DbSource<TInput>(connectionManager, TableName);
            DestinationTable = new DbDestination<TInput>(connectionManager, TableName);
            Init();
        }

        public DbMerge(TableDefinition tableDefinition)
        {
            DestinationTableDefinition = tableDefinition;
            TableName = tableDefinition.Name;
            DestinationTableAsSource = new DbSource<TInput>()
            {
                SourceTableDefinition = DestinationTableDefinition
            };
            Init();
        }

        private void Init()
        {
            GetIdColumName();
            InitInternalFlow();
            InitOutputFlow();
        }

        private void GetIdColumName()
        {
            TypeInfo = new DBMergeTypeInfo(typeof(TInput));

        }


        private void InitInternalFlow()
        {
            Lookup = new LookupTransformation<TInput, TInput>(
                DestinationTableAsSource,
                row => UpdateRowWithDeltaInfo(row)
            );

            DestinationTable.BeforeBatchWrite = batch =>
            {

                DeltaTable.AddRange(batch);
                if (!UseTruncateMethod)
                {
                    SqlDeleteIds(batch.Where(row => row.ChangeAction != "I" && row.ChangeAction != "E"));
                    return batch.Where(row => row.ChangeAction == "I" || row.ChangeAction == "U").ToArray();
                }
                else
                {
                    TruncateDestinationOnce();
                    return batch.Where(row => row.ChangeAction == "I" || row.ChangeAction == "U" || row.ChangeAction == "E").ToArray();
                }
            };

            Lookup.LinkTo(DestinationTable);
        }

        private void InitOutputFlow()
        {
            int x = 0;
            OutputSource = new CustomSource<TInput>(() =>
            {
                return DeltaTable.ElementAt(x++);
            }, () => x >= DeltaTable.Count);

            DestinationTable.OnCompletion = () =>
            {
                IdentifyAndDeleteMissingEntries();
                OutputSource.Execute();
            };
        }

        private TInput UpdateRowWithDeltaInfo(TInput row)
        {
            row.ChangeDate = DateTime.Now;
            row.ChangeAction = "I";
            TInput find = InputData.Where(d => d.UniqueId == row.UniqueId).FirstOrDefault();
            if (find != null)
            {
                if (row.Equals(find))
                {
                    row.ChangeAction = "E";
                    row.ChangeDate = DateTime.Now;
                    find.ChangeAction = "E";
                }
                else
                {
                    row.ChangeAction = "U";
                    row.ChangeDate = DateTime.Now;
                    find.ChangeAction = "U";
                }
            }

            return row;
        }

        void TruncateDestinationOnce()
        {
            if (WasTruncationExecuted == true) return;
            WasTruncationExecuted = true;
            if (DisableDeletion == true) return;
            TruncateTableTask.Truncate(this.ConnectionManager, TableName);
        }

        void IdentifyAndDeleteMissingEntries()
        {
            if (DisableDeletion == true) return;
            var deletions = InputData.Where(row => String.IsNullOrEmpty(row.ChangeAction)).ToList();
            if (!UseTruncateMethod)
                SqlDeleteIds(deletions);
            deletions.ForEach(row =>
            {
                row.ChangeAction = "D";
                row.ChangeDate = DateTime.Now;
            });
            DeltaTable.AddRange(deletions);
        }

        private void SqlDeleteIds(IEnumerable<TInput> rowsToDelete)
        {
            var idsToDelete = rowsToDelete.Select(row => $"'{row.UniqueId}'");
            if (idsToDelete.Count() > 0)
            {
                string idNames = $"{QB}{TypeInfo.IdColumnNames.First()}{QE}";
                if (TypeInfo.IdColumnNames.Count > 1)
                    idNames = CreateConcatSqlForNames();
                new SqlTask(this, $@"
            DELETE FROM {TN.QuotatedFullName} 
            WHERE {idNames} IN (
            {String.Join(",", idsToDelete)}
            )")
                {
                    DisableLogging = true,
                }.ExecuteNonQuery();
            }
        }

        private string CreateConcatSqlForNames()
        {
            string result = $"CONCAT( {string.Join(",", TypeInfo?.IdColumnNames.Select(cn => $"{QB}{cn}{QE}"))} )";
            if (this.ConnectionType == ConnectionManagerType.SQLite)
                result = $" {string.Join("||", TypeInfo?.IdColumnNames.Select(cn => $"{QB}{cn}{QE}"))} ";
            return result;
        }

        public void Wait() => DestinationTable.Wait();
        public async Task Completion() => await DestinationTable.Completion;
    }
}
