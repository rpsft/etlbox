﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks.Dataflow;
using System.Linq;
using ALE.ETLBox.ControlFlow;

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
    public class DBMerge<TInput> : DataFlowTask, ITask, IDataFlowLinkTarget<TInput>,IDataFlowSource<TInput> where TInput : IMergable, new()
    {


        /* ITask Interface */
        public override string TaskType { get; set; } = "DF_DBMERGE";
        public override string TaskName { get; set; } = "Dataflow: Insert, Upsert or delete in destination";
        public override void Execute() => OutputSource.Execute();

        /* Public Properties */
        public ITargetBlock<TInput> TargetBlock => Lookup.TargetBlock;
        public bool DisableDeletion { get; set; }
        public TableDefinition DestinationTableDefinition { get; set; }
        public bool HasDestinationTableDefinition => DestinationTableDefinition != null;
        public string TableName { get; set; }
        public bool HasTableName => !String.IsNullOrWhiteSpace(TableName);
        public List<TInput> DeltaTable { get; set; } = new List<TInput>();

        /* Private stuff */
        Lookup<TInput, TInput, TInput> Lookup { get; set; }
        DBSource<TInput> DestinationTableAsSource { get; set; }
        DBDestination<TInput> DestinationTable { get; set; }
        List<TInput> InputData { get; set; } = new List<TInput>();
        CustomSource<TInput> OutputSource { get; set; }
        bool WasDeletionExecuted { get; set; }
        string MergeIdColumnName { get; set; }
        bool UseTruncateMethod => String.IsNullOrWhiteSpace(MergeIdColumnName);
        NLog.Logger NLogger { get; set; }

        public DBMerge(string tableName)
        {
            TableName = tableName;
            DestinationTableAsSource = new DBSource<TInput>(TableName);
            DestinationTable = new DBDestination<TInput>(TableName);
            Init();
        }

        private void GetIdColumName()
        {
            TypeInfo typeInfo = new TypeInfo(typeof(TInput));
            MergeIdColumnName = typeInfo.MergeIdColumnName;
        }

        public DBMerge(TableDefinition tableDefinition)
        {
            DestinationTableDefinition = tableDefinition;
            TableName = tableDefinition.Name;
            DestinationTableAsSource = new DBSource<TInput>(DestinationTableDefinition);
            Init();
        }

        private void Init()
        {
            GetIdColumName();
            InitInternalFlow();
            InitOutputFlow();
        }

        private void InitInternalFlow()
        {
            Lookup = new Lookup<TInput, TInput, TInput>(
                row => UpdateRowWithDeltaInfo(row),
                DestinationTableAsSource,
                InputData
            );

            DestinationTable.BeforeBatchWrite = batch =>
            {
                DeleteMissingEntriesOnce();
                DeltaTable.AddRange(batch);
                if (!UseTruncateMethod)
                    SqlDeleteIds(batch.Where(row => row.ChangeAction != 'I' && row.ChangeAction != 'E'));
                return batch.Where(row => row.ChangeAction == 'I' || row.ChangeAction == 'U').ToArray();
            };

            Lookup.LinkTo(DestinationTable);
        }

        private void InitOutputFlow()
        {
            int x = 0;
            OutputSource = new CustomSource<TInput>(() =>
            {
                return  DeltaTable.ElementAt(x++);
            }, () => x >= DeltaTable.Count);

            DestinationTable.OnCompletion = () => OutputSource.Execute();
            //OutputSource.LinkTo(new VoidDestination<TInput>());
        }

        private TInput UpdateRowWithDeltaInfo(TInput row)
        {
            row.ChangeDate = DateTime.Now;
            row.ChangeAction = 'I';
            var find = InputData.Where(d => d.UniqueId == row.UniqueId).FirstOrDefault();
            if (find != null)
            {
                if (row.Equals(find))
                {
                    row.ChangeAction = 'E';
                    row.ChangeDate = DateTime.Now;
                    find.ChangeAction = 'E';
                }
                else
                {
                    row.ChangeAction = 'U';
                    row.ChangeDate = DateTime.Now;
                    find.ChangeAction = 'U';
                }
            }

            return row;
        }

        void DeleteMissingEntriesOnce()
        {
            var deletions = InputData.Where(row => row.ChangeAction == 0);
            if (DisableDeletion == false && WasDeletionExecuted == false) {
                if (UseTruncateMethod)
                    TruncateTableTask.Truncate(TableName);
                else
                    SqlDeleteIds(deletions);
            }
            DeltaTable.AddRange(deletions);
            DeltaTable.ForEach(row => {
                row.ChangeAction = 'D';
                row.ChangeDate = DateTime.Now;
            });
            WasDeletionExecuted = true;
        }

        private void SqlDeleteIds(IEnumerable<TInput> rowsToDelete)
        {
            var idsToDelete = rowsToDelete.Select(row => $"'{row.UniqueId}'");
            if (idsToDelete.Count() > 0)
            {
                new SqlTask(this, $@"
            DELETE FROM {TableName} 
            WHERE {MergeIdColumnName} IN (
            {String.Join(",", idsToDelete)}
            )")
                {
                    DisableLogging = true,
                    DisableExtension = true,
                }.ExecuteNonQuery();
            }
        }

        public void Wait() => DestinationTable.Wait();

        public void LinkTo(IDataFlowLinkTarget<TInput> target) => OutputSource.LinkTo(target);

        public void LinkTo(IDataFlowLinkTarget<TInput> target, Predicate<TInput> predicate) => OutputSource.LinkTo(target);

        public void ExecuteAsync() => OutputSource.ExecuteAsync();

        public ISourceBlock<TInput> SourceBlock => OutputSource.SourceBlock;

    }


}