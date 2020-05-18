using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using CsvHelper.Expressions;
using System;
using System.Collections.Generic;
using System.Dynamic;
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
    public class DbMerge<TInput> : DataFlowTransformation<TInput, TInput>, ITask, IDataFlowTransformation<TInput, TInput>, IDataFlowBatchDestination<TInput>
    //where TInput : IMergeableRow, new()
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = "Insert, update or delete in destination";

        public async Task ExecuteAsync() => await OutputSource.ExecuteAsync();
        public void Execute() => OutputSource.Execute();

        /* Public Properties */
        public override ISourceBlock<TInput> SourceBlock => OutputSource.SourceBlock;
        public override ITargetBlock<TInput> TargetBlock => Lookup.TargetBlock;
        public DeltaMode DeltaMode { get; set; }
        public TableDefinition DestinationTableDefinition { get; set; }
        public string TableName { get; set; }

        public override IConnectionManager ConnectionManager
        {
            get => base.ConnectionManager;
            set
            {
                base.ConnectionManager = value;
                DestinationTableAsSource.ConnectionManager = value;
                DestinationTable.ConnectionManager = value;
            }
        }
        public List<TInput> DeltaTable { get; set; } = new List<TInput>();
        public bool UseTruncateMethod
        {
            get
            {
                if (TypeInfo?.IdColumnNames == null
                    || TypeInfo?.IdColumnNames?.Count == 0) return true;
                return _useTruncateMethod;
            }
            set
            {
                _useTruncateMethod = value;
            }
        }

        public int BatchSize
        {
            get => DestinationTable.BatchSize;
            set => DestinationTable.BatchSize = value;
        }

        public DynamicObjectPropNames PropNames { get; set; } = new DynamicObjectPropNames();

        /* Private stuff */
        bool _useTruncateMethod;

        ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, QB, QE);
        LookupTransformation<TInput, TInput> Lookup { get; set; }
        DbSource<TInput> DestinationTableAsSource { get; set; }
        DbDestination<TInput> DestinationTable { get; set; }
        List<TInput> InputData => Lookup.LookupData;
        Dictionary<string, TInput> InputDataDict { get; set; }
        CustomSource<TInput> OutputSource { get; set; }
        bool WasTruncationExecuted { get; set; }
        DBMergeTypeInfo TypeInfo { get; set; }

        public DbMerge(string tableName)
        {
            TableName = tableName;
            Init();
        }

        public DbMerge(IConnectionManager connectionManager, string tableName) : this(tableName)
        {
            ConnectionManager = connectionManager;
        }

        public DbMerge(string tableName, int batchSize) : this(tableName)
        {
            TableName = tableName;
            Init(batchSize);
        }

        public DbMerge(IConnectionManager connectionManager, string tableName, int batchSize) : this(tableName, batchSize)
        {
            ConnectionManager = connectionManager;
        }

        private void Init(int batchSize = DbDestination.DEFAULT_BATCH_SIZE)
        {
            TypeInfo = new DBMergeTypeInfo(typeof(TInput));
            DestinationTableAsSource = new DbSource<TInput>(ConnectionManager, TableName);
            DestinationTable = new DbDestination<TInput>(ConnectionManager, TableName, batchSize);
            InitInternalFlow();
            InitOutputFlow();
        }

        public ChangeAction? GetChangeAction(TInput row)
        {
            if (row is IMergeableRow)
                return ((IMergeableRow)row).ChangeAction;
            else if (TypeInfo.IsDynamic)
            {
                var r = row as IDictionary<string, object>;
                if (!r.ContainsKey("ChangeAction"))
                    r.Add("ChangeAction", null as ChangeAction?);
                 return r["ChangeAction"] as ChangeAction?;
            }
            else
                throw new NotImplementedException();
        }

        public void SetChangeAction(TInput row, ChangeAction? changeAction)
        {
            if (row is IMergeableRow)
                ((IMergeableRow)row).ChangeAction = changeAction;
            else if (TypeInfo.IsDynamic)
            {
                dynamic r = row as ExpandoObject;
                r.ChangeAction = changeAction;
            }
            else
                throw new NotImplementedException();
        }

        public string GetUniqueId(TInput row)
        {
            if (row is IMergeableRow)
            {
                string result = "";
                foreach (var propInfo in TypeInfo.IdAttributeProps)
                    result += propInfo?.GetValue(this).ToString();
                return result;
            }
            else if (TypeInfo.IsDynamic)
            {
                string idColumn = PropNames.IdColumns.FirstOrDefault();
                var r = row as IDictionary<string, object>;
                if (!r.ContainsKey(idColumn))
                    r.Add(idColumn, null);
                return r[idColumn].ToString();
            }
            else
                throw new NotImplementedException();
        }

        public bool GetIsDeletion(TInput row)
        {
            if (row is IMergeableRow)
            {
                bool result = true;
                foreach (var tup in TypeInfo.DeleteAttributeProps)
                    result &= (tup.Item1?.GetValue(this)).Equals(tup.Item2);
                return result;
            }
            else
                throw new NotImplementedException();
        }

        public void SetChangeDate(TInput row, DateTime changeDate)
        {
            if (row is IMergeableRow)
                ((IMergeableRow)row).ChangeDate = changeDate;
            else if (TypeInfo.IsDynamic)
            {
                dynamic r = row as ExpandoObject;
                r.ChangeDate = changeDate;
            }
            else
                throw new NotImplementedException();
        }

        public bool AreEqual(object self, object other)
        {
            if (other == null || self == null) return false;
            if (self is IMergeableRow && other is IMergeableRow)
            {
                bool result = true;
                foreach (var propInfo in TypeInfo.CompareAttributeProps)
                    result &= (propInfo?.GetValue(self))?.Equals(propInfo?.GetValue(other)) ?? false;
                return result;
            }
            else if (TypeInfo.IsDynamic)
            {
                string compColumn = PropNames.CompareColumns.FirstOrDefault();
                var s = self as IDictionary<string, object>;
                var o = other as IDictionary<string, object>;
                return s[compColumn].Equals(o[compColumn]);
            }
            else
                throw new NotImplementedException();
        }

            private void InitInternalFlow()
        {
            Lookup = new LookupTransformation<TInput, TInput>(
                DestinationTableAsSource,
                row => UpdateRowWithDeltaInfo(row)
            );

            DestinationTable.BeforeBatchWrite = batch =>
            {
                if (DeltaMode == DeltaMode.Delta)
                    DeltaTable.AddRange(batch.Where(row => GetChangeAction(row) != ChangeAction.Delete));
                else
                    DeltaTable.AddRange(batch);

                if (!UseTruncateMethod)
                {
                    SqlDeleteIds(batch.Where(row => GetChangeAction(row) != ChangeAction.Insert && GetChangeAction(row) != ChangeAction.Exists));
                    return batch.Where(row => GetChangeAction(row) == ChangeAction.Insert ||
                        GetChangeAction(row) == ChangeAction.Update)
                    .ToArray();
                }
                else
                {
                    TruncateDestinationOnce();
                    return batch.Where(row => GetChangeAction(row) == ChangeAction.Insert ||
                        GetChangeAction(row) == ChangeAction.Update ||
                        GetChangeAction(row) == ChangeAction.Exists)
                    .ToArray();
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
            if (InputDataDict == null) InitInputDataDictionary();
            SetChangeDate(row, DateTime.Now);
            TInput find = default(TInput);
            InputDataDict.TryGetValue(GetUniqueId(row), out find);
            if (DeltaMode == DeltaMode.Delta && GetIsDeletion(row))
            {
                if (find != null)
                {
                    SetChangeAction(find, ChangeAction.Delete);
                    SetChangeAction(row, ChangeAction.Delete);
                }
            }
            else
            {
                SetChangeAction(row, ChangeAction.Insert);
                if (find != null)
                {
                    if (AreEqual(row,find))
                    {
                        SetChangeAction(row, ChangeAction.Exists);
                        SetChangeAction(find, ChangeAction.Exists);
                    }
                    else
                    {
                        SetChangeAction(row, ChangeAction.Update);
                        SetChangeAction(find, ChangeAction.Update);
                    }
                }
            }
            return row;
        }

        private void InitInputDataDictionary()
        {
            InputDataDict = new Dictionary<string, TInput>();
            foreach (var d in InputData)
                InputDataDict.Add(GetUniqueId(d), d);
        }

        void TruncateDestinationOnce()
        {
            if (WasTruncationExecuted == true) return;
            WasTruncationExecuted = true;
            if (DeltaMode == DeltaMode.NoDeletions == true) return;
            TruncateTableTask.Truncate(this.ConnectionManager, TableName);
        }

        void IdentifyAndDeleteMissingEntries()
        {
            if (DeltaMode == DeltaMode.NoDeletions) return;
            IEnumerable<TInput> deletions = null;
            if (DeltaMode == DeltaMode.Delta)
                deletions = InputData.Where(row => GetChangeAction(row) == ChangeAction.Delete).ToList();
            else
                deletions = InputData.Where(row => GetChangeAction(row) == null).ToList();
            if (!UseTruncateMethod)
                SqlDeleteIds(deletions);
            foreach (var row in deletions) //.ForEach(row =>
            {
                SetChangeAction(row, ChangeAction.Delete);
                SetChangeDate(row, DateTime.Now);
            };
            DeltaTable.AddRange(deletions);
        }

        private void SqlDeleteIds(IEnumerable<TInput> rowsToDelete)
        {
            if (rowsToDelete.Count() == 0) return;
            var deleteString = rowsToDelete.Select(row => $"'{GetUniqueId(row)}'");
            string idNames = $"{QB}{TypeInfo.IdColumnNames.First()}{QE}";
            if (TypeInfo.IdColumnNames.Count > 1)
                idNames = CreateConcatSqlForNames();
            new SqlTask(this, $@"
            DELETE FROM {TN.QuotatedFullName} 
            WHERE {idNames} IN (
            {String.Join(",", deleteString)}
            )")
            {
                DisableLogging = true,
            }.ExecuteNonQuery();
        }

        private string CreateConcatSqlForNames()
        {
            string result = $"CONCAT( {string.Join(",", TypeInfo?.IdColumnNames.Select(cn => $"{QB}{cn}{QE}"))} )";
            if (this.ConnectionType == ConnectionManagerType.SQLite)
                result = $" {string.Join("||", TypeInfo?.IdColumnNames.Select(cn => $"{QB}{cn}{QE}"))} ";
            return result;
        }

        public void Wait() => DestinationTable.Wait();
        public Task Completion => DestinationTable.Completion;
    }

    public enum DeltaMode
    {
        Full = 0,
        NoDeletions = 1,
        Delta = 2,
    }

    public class DynamicObjectPropNames
    {
        public List<string> IdColumns { get; set; } = new List<string>();
        public List<string> CompareColumns { get; set; } = new List<string>();
        public Dictionary<string, object> DeletionColumns { get; set; } = new Dictionary<string, object>();

    }
}
