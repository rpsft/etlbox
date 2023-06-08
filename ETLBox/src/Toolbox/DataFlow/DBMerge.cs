using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;

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
    [PublicAPI]
    public class DbMerge<TInput>
        : DataFlowTransformation<TInput, TInput>,
            IDataFlowBatchDestination<TInput>
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

        public List<TInput> DeltaTable { get; set; } = new();
        public bool UseTruncateMethod
        {
            get
            {
                if (IdColumnNames == null || IdColumnNames?.Count == 0)
                    return true;
                return _useTruncateMethod;
            }
            set { _useTruncateMethod = value; }
        }

        private bool _useTruncateMethod;

        public int BatchSize
        {
            get => DestinationTable.BatchSize;
            set => DestinationTable.BatchSize = value;
        }

        public MergeProperties MergeProperties { get; set; } = new();

        /* Private stuff */
        private List<string> IdColumnNames =>
            MergeProperties.IdPropertyNames?.Count > 0
                ? MergeProperties.IdPropertyNames
                : TypeInfo?.IdColumnNames;

        private ObjectNameDescriptor TN => new(TableName, QB, QE);
        private LookupTransformation<TInput, TInput> Lookup { get; set; }
        private DbSource<TInput> DestinationTableAsSource { get; set; }
        private DbDestination<TInput> DestinationTable { get; set; }
        private List<TInput> InputData => Lookup.LookupData;
        private Dictionary<string, TInput> InputDataDict { get; set; }
        private CustomSource<TInput> OutputSource { get; set; }
        private bool WasTruncationExecuted { get; set; }
        private DBMergeTypeInfo TypeInfo { get; set; }

        public DbMerge(string tableName)
            : this(null, tableName) { }

        public DbMerge(
            IConnectionManager connectionManager,
            string tableName,
            int batchSize = DbDestination.DEFAULT_BATCH_SIZE
        )
        {
            TableName = tableName;
            DestinationTableAsSource = new DbSource<TInput>(ConnectionManager, TableName);
            DestinationTable = new DbDestination<TInput>(ConnectionManager, TableName, batchSize);
            InitInternalFlow();
            InitOutputFlow();
            if (connectionManager != null)
                ConnectionManager = connectionManager;
        }

        protected sealed override void OnConnectionManagerChanged(IConnectionManager value)
        {
            DestinationTableAsSource.ConnectionManager = value;
            DestinationTable.ConnectionManager = value;
            base.OnConnectionManagerChanged(value);
        }

        public ChangeAction? GetChangeAction(TInput row)
        {
            //if (row is IMergeableRow)
            //    return ((IMergeableRow)row).ChangeAction;
            //else
            if (TypeInfo.IsDynamic)
            {
                var r = row as IDictionary<string, object>;
                if (!r!.ContainsKey("ChangeAction"))
                    r.Add("ChangeAction", null as ChangeAction?);
                return r["ChangeAction"] as ChangeAction?;
            }

            if (TypeInfo.ChangeActionProperty != null)
            {
                return TypeInfo.ChangeActionProperty.GetValue(row) as ChangeAction?;
            }

            throw new ETLBoxNotSupportedException(
                "Objects used for merge must inherit from MergeableRow or"
                    + "contain a property ChangeAction (public ChangeAction? ChangeAction {get;set;}"
            );
        }

        public void SetChangeAction(TInput row, ChangeAction? changeAction)
        {
            //if (row is IMergeableRow)
            //    ((IMergeableRow)row).ChangeAction = changeAction;
            if (TypeInfo.IsDynamic)
            {
                dynamic r = row as ExpandoObject;
                r!.ChangeAction = changeAction;
            }
            else if (TypeInfo.ChangeActionProperty != null)
            {
                TypeInfo.ChangeActionProperty.SetValueOrThrow(row, changeAction);
            }
            else
                throw new ETLBoxNotSupportedException(
                    "Objects used for merge must inherit from MergeableRow or"
                        + "contain a property ChangeAction (public ChangeAction? ChangeAction {get;set;}"
                );
        }

        public string GetUniqueId(TInput row)
        {
            string result = "";
            if (TypeInfo.IsDynamic && MergeProperties.IdPropertyNames.Count > 0)
            {
                var r = row as IDictionary<string, object>;
                foreach (string idColumn in MergeProperties.IdPropertyNames)
                {
                    if (!r!.ContainsKey(idColumn))
                        r.Add(idColumn, null);
                    result += r[idColumn].ToString();
                }
                return result;
            }

            if (TypeInfo.IdAttributeProps.Count > 0)
            {
                foreach (var propInfo in TypeInfo.IdAttributeProps)
                    result += propInfo?.GetValue(row).ToString();
                return result;
            }

            throw new ETLBoxNotSupportedException(
                "Objects used for merge must at least define a id column"
                    + "to identify matching rows - please use the IdColumn attribute or add a property name in the MergeProperties.IdProperyNames list."
            );
        }

        public bool GetIsDeletion(TInput row)
        {
            if (TypeInfo.IsDynamic && MergeProperties.DeletionProperties.Count > 0)
            {
                return FindDeletionDynamicProperty(row as IDictionary<string, object>);
            }

            if (TypeInfo.DeleteAttributeProps.Count > 0)
            {
                bool result = true;
                foreach (var tup in TypeInfo.DeleteAttributeProps)
                    result &= tup.Item1?.GetValue(row)?.Equals(tup.Item2) ?? false;
                return result;
            }

            return false;
        }

        private bool FindDeletionDynamicProperty(IDictionary<string, object> r)
        {
            bool result = true;
            foreach (var delColumn in MergeProperties.DeletionProperties)
            {
                if (r!.ContainsKey(delColumn.Key))
                    result &= r[delColumn.Key]?.Equals(delColumn.Value) ?? true;
                else
                    result = false;
            }

            return result;
        }

        public void SetChangeDate(TInput row, DateTime changeDate)
        {
            if (TypeInfo.IsDynamic)
            {
                dynamic r = row as ExpandoObject;
                r!.ChangeDate = changeDate;
            }
            else if (TypeInfo.ChangeDateProperty != null)
            {
                TypeInfo.ChangeDateProperty.SetValueOrThrow(row, changeDate);
            }
            else
                throw new ETLBoxNotSupportedException(
                    "Objects used for merge must inherit from MergeableRow or "
                        + "contain a property ChangeDate (public DateTime ChangeDate {get;set;}"
                );
        }

        public bool AreEqual(object self, object other)
        {
            if (other == null || self == null)
                return false;
            if (TypeInfo.IsDynamic)
            {
                return CompareDynamicObjects(
                    self as IDictionary<string, object>,
                    other as IDictionary<string, object>
                );
            }

            if (TypeInfo.CompareAttributeProps.Count > 0)
            {
                bool result = true;
                foreach (var propInfo in TypeInfo.CompareAttributeProps)
                    result &= propInfo?.GetValue(self)?.Equals(propInfo.GetValue(other)) ?? false;
                return result;
            }

            return false;
        }

        private bool CompareDynamicObjects(
            IDictionary<string, object> self,
            IDictionary<string, object> other
        )
        {
            bool result = true;
            foreach (string compColumn in MergeProperties.ComparePropertyNames)
                if (self!.ContainsKey(compColumn) && other!.ContainsKey(compColumn))
                    result &= self[compColumn]?.Equals(other[compColumn]) ?? false;
            return result;
        }

        private void InitInternalFlow()
        {
            Lookup = new LookupTransformation<TInput, TInput>(
                DestinationTableAsSource,
                UpdateRowWithDeltaInfo
            );

            DestinationTable.BeforeBatchWrite = batch =>
            {
                DeltaTable.AddRange(
                    DeltaMode == DeltaMode.Delta
                        ? batch.Where(row => GetChangeAction(row) != ChangeAction.Delete)
                        : batch
                );

                if (!UseTruncateMethod)
                {
                    SqlDeleteIds(
                        batch.Where(
                            row =>
                                GetChangeAction(row) != ChangeAction.Insert
                                && GetChangeAction(row) != ChangeAction.Exists
                        )
                    );
                    return batch
                        .Where(
                            row =>
                                GetChangeAction(row) == ChangeAction.Insert
                                || GetChangeAction(row) == ChangeAction.Update
                        )
                        .ToArray();
                }

                if (DeltaMode == DeltaMode.Delta)
                    throw new ETLBoxNotSupportedException(
                        "If you provide a delta load, you must define at least one compare column."
                            + "Using the truncate method is not allowed. "
                    );
                TruncateDestinationOnce();
                return batch
                    .Where(
                        row =>
                            GetChangeAction(row) == ChangeAction.Insert
                            || GetChangeAction(row) == ChangeAction.Update
                            || GetChangeAction(row) == ChangeAction.Exists
                    )
                    .ToArray();
            };

            Lookup.LinkTo(DestinationTable);
        }

        private void InitOutputFlow()
        {
            int x = 0;
            OutputSource = new CustomSource<TInput>(
                () => DeltaTable.ElementAt(x++),
                () => x >= DeltaTable.Count
            );

            DestinationTable.OnCompletion = () =>
            {
                IdentifyAndDeleteMissingEntries();
                OutputSource.Execute();
            };
        }

        private TInput UpdateRowWithDeltaInfo(TInput row)
        {
            if (!_wasTypeInfoInitialized)
                InitTypeInfo();
            if (InputDataDict == null)
                InitInputDataDictionary();
            SetChangeDate(row, DateTime.Now);
            InputDataDict!.TryGetValue(GetUniqueId(row), out TInput find);
            if (DeltaMode == DeltaMode.Delta && GetIsDeletion(row))
            {
                SetDeleteAction(row, find);
            }
            else
            {
                SetInsertUpdateAction(row, find);
            }
            return row;
        }

        private void SetInsertUpdateAction(TInput row, TInput find)
        {
            SetChangeAction(row, ChangeAction.Insert);
            if (find != null)
            {
                if (AreEqual(row, find))
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

        private void SetDeleteAction(TInput row, TInput find)
        {
            if (find != null)
            {
                SetChangeAction(find, ChangeAction.Delete);
                SetChangeAction(row, ChangeAction.Delete);
            }
        }

        private void InitInputDataDictionary()
        {
            InputDataDict = new Dictionary<string, TInput>();
            foreach (var d in InputData)
                InputDataDict.Add(GetUniqueId(d), d);
        }

        private bool _wasTypeInfoInitialized;

        private void InitTypeInfo()
        {
            TypeInfo = new DBMergeTypeInfo(typeof(TInput), MergeProperties);
            _wasTypeInfoInitialized = true;
        }

        private void TruncateDestinationOnce()
        {
            if (WasTruncationExecuted)
                return;
            WasTruncationExecuted = true;
            if (DeltaMode == DeltaMode.NoDeletions)
                return;
            TruncateTableTask.Truncate(ConnectionManager, TableName);
        }

        private void IdentifyAndDeleteMissingEntries()
        {
            if (DeltaMode == DeltaMode.NoDeletions)
                return;
            IEnumerable<TInput> deletions;
            if (DeltaMode == DeltaMode.Delta)
                deletions = InputData
                    .Where(row => GetChangeAction(row) == ChangeAction.Delete)
                    .ToList();
            else
                deletions = InputData.Where(row => GetChangeAction(row) == null).ToList();
            if (!UseTruncateMethod)
                SqlDeleteIds(deletions);
            foreach (var row in deletions) //.ForEach(row =>
            {
                SetChangeAction(row, ChangeAction.Delete);
                SetChangeDate(row, DateTime.Now);
            }
            DeltaTable.AddRange(deletions);
        }

        private void SqlDeleteIds(IEnumerable<TInput> rowsToDelete)
        {
            var delete = rowsToDelete as TInput[] ?? rowsToDelete.ToArray();
            if (!delete.Any())
                return;
            var deleteString = delete.Select(row => $"'{GetUniqueId(row)}'");
            string idNames = $"{QB}{IdColumnNames.First()}{QE}";
            if (IdColumnNames.Count > 1)
                idNames = CreateConcatSqlForNames();
            new SqlTask(
                this,
                $@"
            DELETE FROM {TN.QuotatedFullName} 
            WHERE {idNames} IN (
            {string.Join(",", deleteString)}
            )"
            )
            {
                DisableLogging = true,
            }.ExecuteNonQuery();
        }

        private string CreateConcatSqlForNames()
        {
            string result =
                $"CONCAT( {string.Join(",", IdColumnNames.Select(cn => $"{QB}{cn}{QE}"))} )";
            if (ConnectionType == ConnectionManagerType.SQLite)
                result = $" {string.Join("||", IdColumnNames.Select(cn => $"{QB}{cn}{QE}"))} ";
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

    public class MergeProperties
    {
        public List<string> IdPropertyNames { get; set; } = new();
        public List<string> ComparePropertyNames { get; set; } = new();
        public Dictionary<string, object> DeletionProperties { get; set; } = new();
        internal string ChangeActionPropertyName { get; set; } = "ChangeAction";
        internal string ChangeDatePropertyName { get; set; } = "ChangeDate";
    }

    /// <inheritdoc/>
    [PublicAPI]
    public class DbMerge : DbMerge<ExpandoObject>
    {
        public DbMerge(string tableName)
            : base(tableName) { }

        public DbMerge(IConnectionManager connectionManager, string tableName)
            : base(connectionManager, tableName) { }

        public DbMerge(string tableName, int batchSize)
            : base(null, tableName, batchSize) { }

        public DbMerge(IConnectionManager connectionManager, string tableName, int batchSize)
            : base(connectionManager, tableName, batchSize) { }
    }
}
