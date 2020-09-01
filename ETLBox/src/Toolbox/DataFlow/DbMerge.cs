using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow.Transformations;
using ETLBox.Exceptions;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// Inserts, updates and (optionally) deletes data in database target.
    /// Before the Merge is executed, all data from the destination is read into memory.
    /// A delta table is generated that stores information if a records was inserted, updated, deleted or hasn't been touched (existed).
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data.</typeparam>
    public class DbMerge<TInput> : DataFlowTransformation<TInput, TInput>, IDataFlowBatchDestination<TInput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = "Insert, update or delete in destination";
        /// <inheritdoc/>
        public override ISourceBlock<TInput> SourceBlock => OutputSource.SourceBlock;
        /// <inheritdoc/>
        public override ITargetBlock<TInput> TargetBlock => Lookup.TargetBlock;

        /// <summary>
        /// Defines the type of target data which affects how deletions or insertions are handled.
        /// Full means that source contains all data, NoDeletions that source contains all data but no deletions are executed,
        /// Delta means that source has only delta information and deletions are deferred from a particular property and
        /// OnlyUpdates means that only updates are applied to the destination.
        /// </summary>
        public MergeMode MergeMode { get; set; }

        /// <summary>
        /// The table definition of the destination table. By default, the table definition is read from the database.
        /// Provide a table definition if the definition of the target can't be read automatically or you want the DbMerge
        /// only to use the columns in the provided definition.
        /// </summary>
        public TableDefinition DestinationTableDefinition { get; set; }

        /// <summary>
        /// The name of the target database table for the merge.
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// A list that is filled with delta information when the merge is executed.
        /// It will contain inserted, updated, deleted and untouched (existing) rows.
        /// </summary>
        public List<TInput> DeltaTable { get; set; } = new List<TInput>();

        /// <summary>
        /// A list of property names that are used in the Merge.
        /// </summary>
        public MergeProperties MergeProperties { get; set; } = new MergeProperties();

        /// <summary>
        /// By default, only records are deleted that either need to be deleted or inserted by using a DELETE FROM statement.
        /// If this property is set to true, all records are delete before using a TRUNCATE, then subsequently records are reinserted again.
        /// This can be faster if many records would need to be deleted from the destination.
        /// </summary>
        public bool UseTruncateMethod
        {
            get
            {
                if (IdColumnNames == null
                    || IdColumnNames?.Count == 0
                    )
                    return true;
                return _useTruncateMethod;
            }
            set
            {
                _useTruncateMethod = value;
            }
        }
        bool _useTruncateMethod;

        /// <summary>
        /// The batch size used when inserted data into the database table.
        /// </summary>
        public int BatchSize { get; set; } = DataFlowBatchDestination<TInput>.DEFAULT_BATCH_SIZE;

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

        public void Wait() => Completion.Wait();

        #region Constructors

        public DbMerge(string tableName)
        {
            TableName = tableName;
            DestinationTableAsSource = new DbSource<TInput>();
            DestinationTable = new DbDestination<TInput>();
            Lookup = new LookupTransformation<TInput, TInput>();
            OutputSource = new CustomSource<TInput>();
        }

        public DbMerge(IConnectionManager connectionManager, string tableName) : this(tableName)
        {
            ConnectionManager = connectionManager;
        }

        public DbMerge(string tableName, int batchSize) : this(tableName)
        {
            BatchSize = batchSize;
        }

        public DbMerge(IConnectionManager connectionManager, string tableName, int batchSize) : this(tableName, batchSize)
        {
            ConnectionManager = connectionManager;
        }

        #endregion

        #region Implement abstract methods

        internal override Task BufferCompletion => Task.WhenAll(Lookup.Completion, DestinationTable.Completion);

        internal override void CompleteBufferOnPredecessorCompletion() => Lookup.CompleteBufferOnPredecessorCompletion();

        internal override void FaultBufferOnPredecessorCompletion(Exception e) {
            Lookup.FaultBufferOnPredecessorCompletion(e);
            OutputSource.FaultBufferOnPredecessorCompletion(e);
        }

        protected override void InternalInitBufferObjects()
        {
            InitTypeInfoWithMergeProperties();
            SetDestinationTableProperties();
            SetLookupProperties();
            SetOutputFlow();
            CreateAndRunLookupToDestinationTableFlow();
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        public new IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
        {
            var errorSource = InternalLinkErrorTo(target);
            Lookup.ErrorSource = new ErrorSource() { Redirection = this.ErrorSource };
            DestinationTable.ErrorSource = new ErrorSource() { Redirection = this.ErrorSource };
            OutputSource.ErrorSource = new ErrorSource() { Redirection = this.ErrorSource };
            return errorSource;
        }

        #endregion

        #region Internal flow initialization

        private void InitTypeInfoWithMergeProperties()
        {
            TypeInfo = new DBMergeTypeInfo(typeof(TInput), MergeProperties);
        }

        private void SetDestinationTableProperties()
        {
            DestinationTable.ConnectionManager = ConnectionManager;
            DestinationTable.TableName = TableName;
            DestinationTable.BatchSize = BatchSize;
            DestinationTable.MaxBufferSize = this.MaxBufferSize;

            DestinationTable.BeforeBatchWrite = batch =>
            {
                if (MergeMode == MergeMode.Delta)
                    DeltaTable.AddRange(batch.Where(row => GetChangeAction(row) != ChangeAction.Delete));
                else if (MergeMode == MergeMode.OnlyUpdates)
                    DeltaTable.AddRange(batch.Where(row => GetChangeAction(row) == ChangeAction.Exists
                        || GetChangeAction(row) == ChangeAction.Update));
                else
                    DeltaTable.AddRange(batch);

                if (!UseTruncateMethod)
                {
                    if (MergeMode == MergeMode.OnlyUpdates)
                    {
                        SqlDeleteIds(batch.Where(row => GetChangeAction(row) == ChangeAction.Update));
                        return batch.Where(row => GetChangeAction(row) == ChangeAction.Update).ToArray();
                    }
                    else
                    {
                        SqlDeleteIds(batch.Where(row => GetChangeAction(row) != ChangeAction.Insert && GetChangeAction(row) != ChangeAction.Exists));
                        return batch.Where(row => GetChangeAction(row) == ChangeAction.Insert ||
                            GetChangeAction(row) == ChangeAction.Update)
                        .ToArray();
                    }
                }
                else
                {
                    if (MergeMode == MergeMode.Delta)
                        throw new ETLBoxNotSupportedException("If you provide a delta load, you must define at least one compare column." +
                            "Using the truncate method is not allowed. ");
                    TruncateDestinationOnce();
                    if (MergeMode == MergeMode.OnlyUpdates)
                        return batch.Where(row => GetChangeAction(row) != ChangeAction.Delete &&
                            GetChangeAction(row) != ChangeAction.Insert).ToArray();
                    else
                        return batch.Where(row => GetChangeAction(row) == ChangeAction.Insert ||
                            GetChangeAction(row) == ChangeAction.Update ||
                            GetChangeAction(row) == ChangeAction.Exists)
                        .ToArray();
                }
            };

            DestinationTable.OnCompletion = () =>
            {

                IdentifyAndDeleteMissingEntries();
                if (UseTruncateMethod && (MergeMode == MergeMode.OnlyUpdates || MergeMode == MergeMode.NoDeletions))
                    ReinsertTruncatedRecords();
                OutputSource.Execute();
            };
        }

        private void SetLookupProperties()
        {
            Lookup.Source = DestinationTableAsSource;
            Lookup.TransformationFunc = UpdateRowWithDeltaInfo;
            Lookup.MaxBufferSize = this.MaxBufferSize;
            DestinationTableAsSource.ConnectionManager = ConnectionManager;
            DestinationTableAsSource.TableName = TableName;
        }

        private void SetOutputFlow()
        {
            OutputSource.MaxBufferSize = this.MaxBufferSize;
            SetOutputReadFunc();
            OutputSource.InitBufferObjects();
        }

        private void SetOutputReadFunc()
        {
            int x = 0;
            OutputSource.ReadFunc = () =>
            {
                return DeltaTable.ElementAt(x++);
            };
            OutputSource.ReadCompletedFunc = () => x >= DeltaTable.Count;
        }

        private void CreateAndRunLookupToDestinationTableFlow()
        {
            Lookup.LinkTo(DestinationTable);
            Lookup.InitBufferObjects();
            Lookup.Completion = Lookup.BufferCompletion;
            Lookup.InitNetworkRecursively();
        }

        #endregion

        #region Implementation

        List<string> IdColumnNames
        {
            get
            {
                if (MergeProperties.IdPropertyNames?.Count > 0)
                    return MergeProperties.IdPropertyNames;
                else
                    return TypeInfo?.IdColumnNames;
            }
        }
        ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, QB, QE);
        LookupTransformation<TInput, TInput> Lookup;
        DbSource<TInput> DestinationTableAsSource;
        DbDestination<TInput> DestinationTable;
        List<TInput> InputData => Lookup.LookupData;
        Dictionary<string, TInput> InputDataDict;
        CustomSource<TInput> OutputSource;
        bool WasTruncationExecuted;
        DBMergeTypeInfo TypeInfo;

        private ChangeAction? GetChangeAction(TInput row)
        {
            if (TypeInfo.IsDynamic)
            {
                var r = row as IDictionary<string, object>;
                if (!r.ContainsKey("ChangeAction"))
                    r.Add("ChangeAction", null as ChangeAction?);
                return r["ChangeAction"] as ChangeAction?;
            }
            else if (TypeInfo.ChangeActionProperty != null)
            {
                return TypeInfo.ChangeActionProperty.GetValue(row) as ChangeAction?;
            }
            else
                throw new ETLBoxNotSupportedException("Objects used for merge must inherit from MergeableRow or" +
                    "contain a property ChangeAction (public ChangeAction? ChangeAction {get;set;}");
        }

        private void SetChangeAction(TInput row, ChangeAction? changeAction)
        {
            if (TypeInfo.IsDynamic)
            {
                dynamic r = row as ExpandoObject;
                r.ChangeAction = changeAction;
            }
            else if (TypeInfo.ChangeActionProperty != null)
            {
                TypeInfo.ChangeActionProperty.SetValueOrThrow(row, changeAction);
            }
            else
                throw new ETLBoxNotSupportedException("Objects used for merge must inherit from MergeableRow or" +
    "contain a property ChangeAction (public ChangeAction? ChangeAction {get;set;}");
        }

        private string GetUniqueId(TInput row)
        {
            string result = "";
            if (TypeInfo.IsDynamic && MergeProperties.IdPropertyNames.Count > 0)
            {
                var r = row as IDictionary<string, object>;
                foreach (string idColumn in MergeProperties.IdPropertyNames)
                {
                    if (!r.ContainsKey(idColumn))
                        r.Add(idColumn, null);
                    result += r[idColumn].ToString();
                }
                return result;
            }
            else if (TypeInfo.IdAttributeProps.Count > 0)
            {
                foreach (var propInfo in TypeInfo.IdAttributeProps)
                    result += propInfo?.GetValue(row).ToString();
                return result;
            }
            else
                throw new ETLBoxNotSupportedException("Objects used for merge must at least define a id column" +
  "to identify matching rows - please use the IdColumn attribute or add a property name in the MergeProperties.IdProperyNames list.");
        }

        private bool GetIsDeletion(TInput row)
        {
            bool result = true;
            if (TypeInfo.IsDynamic && MergeProperties.DeletionProperties.Count > 0)
            {
                var r = row as IDictionary<string, object>;
                foreach (var delColumn in MergeProperties.DeletionProperties)
                {
                    if (r.ContainsKey(delColumn.Key))
                        result &= r[delColumn.Key]?.Equals(delColumn.Value) ?? false;
                    else
                        result &= false;
                }
                return result;
            }
            else if (TypeInfo.DeleteAttributeProps.Count > 0)
            {
                foreach (var tup in TypeInfo.DeleteAttributeProps)
                    result &= (tup.Item1?.GetValue(row)).Equals(tup.Item2);
                return result;
            }
            else
                return false;
        }

        private void SetChangeDate(TInput row, DateTime changeDate)
        {
            if (TypeInfo.IsDynamic)
            {
                dynamic r = row as ExpandoObject;
                r.ChangeDate = changeDate;
            }
            else if (TypeInfo.ChangeDateProperty != null)
            {
                TypeInfo.ChangeDateProperty.SetValueOrThrow(row, changeDate);
            }
            else
                throw new ETLBoxNotSupportedException("Objects used for merge must inherit from MergeableRow or " +
    "contain a property ChangeDate (public DateTime ChangeDate {get;set;}");
        }

        private bool AreEqual(object self, object other)
        {
            if (other == null || self == null) return false;
            bool result = true;
            if (TypeInfo.IsDynamic)
            {
                var s = self as IDictionary<string, object>;
                var o = other as IDictionary<string, object>;
                foreach (string compColumn in MergeProperties.ComparePropertyNames)
                    if (s.ContainsKey(compColumn) && o.ContainsKey(compColumn))
                        result &= s[compColumn]?.Equals(o[compColumn]) ?? false;
                return result;
            }
            else if (TypeInfo.CompareAttributeProps.Count > 0)
            {
                foreach (var propInfo in TypeInfo.CompareAttributeProps)
                    result &= (propInfo?.GetValue(self))?.Equals(propInfo?.GetValue(other)) ?? false;
                return result;
            }
            else
                return false;
        }

        private TInput UpdateRowWithDeltaInfo(TInput row)
        {
            if (InputDataDict == null) InitInputDataDictionary();
            SetChangeDate(row, DateTime.Now);
            TInput find = default(TInput);
            InputDataDict.TryGetValue(GetUniqueId(row), out find);
            if (MergeMode == MergeMode.Delta && GetIsDeletion(row))
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
            return row;
        }

        private void InitInputDataDictionary()
        {
            InputDataDict = new Dictionary<string, TInput>();
            foreach (var d in InputData)
                InputDataDict.Add(GetUniqueId(d), d);
        }

        private void TruncateDestinationOnce()
        {
            if (WasTruncationExecuted == true) return;
            WasTruncationExecuted = true;
            if (MergeMode == MergeMode.NoDeletions || MergeMode == MergeMode.OnlyUpdates) return;
            TruncateTableTask.Truncate(this.ConnectionManager, TableName);
        }

        private void IdentifyAndDeleteMissingEntries()
        {
            if (MergeMode == MergeMode.NoDeletions || MergeMode == MergeMode.OnlyUpdates) return;
            IEnumerable<TInput> deletions = null;
            if (MergeMode == MergeMode.Delta)
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
            string idNames = $"{QB}{IdColumnNames.First()}{QE}";
            if (IdColumnNames.Count > 1)
                idNames = CreateConcatSqlForNames();
            var sql = new SqlTask($@"
            DELETE FROM {TN.QuotatedFullName} 
            WHERE {idNames} IN (
            {String.Join(",", deleteString)}
            )")
            {
                DisableLogging = true,
                ConnectionManager = this.ConnectionManager
            };
            sql.CopyLogTaskProperties(this);
            sql.ExecuteNonQuery();
        }

        private void ReinsertTruncatedRecords()
        {
            throw new Exception("Using MergeModes OnlyUpdate or NoDeletions is" +
                " currently not supported when the UseTruncateMethod flag is set." +
                " Set UseTruncateMethod to false if choosing these MergeMethods.");
        }

        private string CreateConcatSqlForNames()
        {
            string result = $"CONCAT( {string.Join(",", IdColumnNames.Select(cn => $"{QB}{cn}{QE}"))} )";
            if (this.ConnectionType == ConnectionManagerType.SQLite)
                result = $" {string.Join("||", IdColumnNames.Select(cn => $"{QB}{cn}{QE}"))} ";
            return result;
        }

        #endregion
    }


    public class DbMerge : DbMerge<ExpandoObject>
    {
        public DbMerge(string tableName) : base(tableName)
        { }

        public DbMerge(IConnectionManager connectionManager, string tableName) : base(connectionManager, tableName)
        { }

        public DbMerge(string tableName, int batchSize) : base(tableName, batchSize)
        { }

        public DbMerge(IConnectionManager connectionManager, string tableName, int batchSize) : base(connectionManager, tableName, batchSize)
        { }
    }
}
