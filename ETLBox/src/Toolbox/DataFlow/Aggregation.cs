using System;
using System.Collections.Generic;
using System.Threading.Tasks.Dataflow;


namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Aggregates data by the given aggregation method.
    /// </summary>
    /// <typeparam name="TInput">Type of data input</typeparam>
    /// <typeparam name="TOutput">Type of data output</typeparam>
    public class Aggregation<TInput, TOutput> : DataFlowTransformation<TInput, TOutput>, ITask, IDataFlowTransformation<TInput, TOutput>
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = "Execute aggregation block.";

        /* Public Properties */
        public Action<TInput, TOutput> AggregationAction
        {
            get
            {
                return _aggregationAction;
            }
            set
            {
                _aggregationAction = value;
                InputBuffer = new ActionBlock<TInput>(row => WrapAggregationAction(row));
                InputBuffer.Completion.ContinueWith(t => WriteIntoOutput());
            }
        }
        public Func<TInput, object> GroupingProperty { get; set; }
        public Action<object, TOutput> StoreKeyAction { get; set; }
        public override ISourceBlock<TOutput> SourceBlock => OutputBuffer;
        public override ITargetBlock<TInput> TargetBlock => InputBuffer;

        /* Private stuff */
        BufferBlock<TOutput> OutputBuffer { get; set; }
        ActionBlock<TInput> InputBuffer { get; set; }

        Action<TInput, TOutput> _aggregationAction;
        Dictionary<object, TOutput> AggregationData { get; set; } = new Dictionary<object, TOutput>();

        public Aggregation()
        {
            OutputBuffer = new BufferBlock<TOutput>();
        }

        public Aggregation(Action<TInput, TOutput> aggregationAction) : this()
        {
            AggregationAction = aggregationAction;
        }

        public Aggregation(Action<TInput, TOutput> aggregationAction, Func<TInput, object> groupingProperty)
            : this(aggregationAction)
        {
            GroupingProperty = groupingProperty;
        }

        public Aggregation(Action<TInput, TOutput> aggregationAction, Func<TInput, object> groupingProperty, Action<object, TOutput> storeKeyAction)
            : this(aggregationAction, groupingProperty)
        {
            StoreKeyAction = storeKeyAction;
        }

        private void WriteIntoOutput()
        {
            NLogStart();
            foreach (var row in AggregationData)
            {
                StoreKeyAction?.Invoke(row.Key, row.Value);
                OutputBuffer.SendAsync(row.Value).Wait();
                LogProgress();
            }
            OutputBuffer.Complete();
            NLogFinish();
        }

        private void WrapAggregationAction(TInput row)
        {
            object key = GroupingProperty?.Invoke(row) ?? "AggregateAll";

            if (!AggregationData.ContainsKey(key))
                AddRecordToDict(key);

            TOutput currentAgg = AggregationData[key];
            AggregationAction.Invoke(row, currentAgg);

        }

        private void AddRecordToDict(object key)
        {
            TOutput firstEntry = default(TOutput);
            firstEntry = (TOutput)Activator.CreateInstance(typeof(TOutput));
            AggregationData.Add(key, firstEntry);
        }

    }

    public enum AggregationMethod
    {
        Sum,
        Min,
        Max,
        Avg
    }
}
