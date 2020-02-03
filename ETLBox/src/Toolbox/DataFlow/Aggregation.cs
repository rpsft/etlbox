using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
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
        public Func<TInput, object> GroupingFunc { get; set; }
        public Action<object, TOutput> StoreKeyAction { get; set; }
        public override ISourceBlock<TOutput> SourceBlock => OutputBuffer;
        public override ITargetBlock<TInput> TargetBlock => InputBuffer;


        /* Private stuff */
        BufferBlock<TOutput> OutputBuffer { get; set; }
        ActionBlock<TInput> InputBuffer { get; set; }

        Action<TInput, TOutput> _aggregationAction;
        Dictionary<object, TOutput> AggregationData { get; set; } = new Dictionary<object, TOutput>();
        AggregationTypeInfo AggTypeInfo { get; set; }

        public Aggregation()
        {
            OutputBuffer = new BufferBlock<TOutput>();
            AggTypeInfo = new AggregationTypeInfo(typeof(TInput), typeof(TOutput));

            if (AggregationAction == null && AggTypeInfo.AggregateColumnInInput != null)
                AggregationAction = DefineAggregationAction;

            if (GroupingFunc == null && AggTypeInfo.GroupColumnsInputAndOutput?.Count > 0)
                GroupingFunc = DefineGroupingPropertyFromAttributes;

            if (StoreKeyAction == null && AggTypeInfo.GroupColumnsInputAndOutput?.Count > 0)
                StoreKeyAction = DefineStoreKeyActionFromAttributes;
        }

        public Aggregation(Action<TInput, TOutput> aggregationAction) : this()
        {
            AggregationAction = aggregationAction;
        }

        public Aggregation(Action<TInput, TOutput> aggregationAction, Func<TInput, object> groupingFunc)
            : this(aggregationAction)
        {
            GroupingFunc = groupingFunc;
        }

        public Aggregation(Action<TInput, TOutput> aggregationAction, Func<TInput, object> groupingFunc, Action<object, TOutput> storeKeyAction)
            : this(aggregationAction, groupingFunc)
        {
            StoreKeyAction = storeKeyAction;
        }

        private void DefineAggregationAction(TInput inputrow, TOutput aggOutput)
        {
            decimal? inputVal = ConvertToDecimal(AggTypeInfo.AggregateColumnInInput.GetValue(inputrow));
            decimal? aggVal = ConvertToDecimal(AggTypeInfo.AggregateColumnInOutput.GetValue(aggOutput));
            decimal? res = null;
            if (aggVal == null && AggTypeInfo.AggregationMethod == AggregationMethod.Count)
                res = 1;
            else if (aggVal == null)
                res = inputVal;
            else if (AggTypeInfo.AggregationMethod == AggregationMethod.Sum)
                res = (inputVal ?? 0) + aggVal;
            else if (AggTypeInfo.AggregationMethod == AggregationMethod.Max)
                res = ((inputVal ?? 0) > aggVal) ? inputVal : aggVal;
            else if (AggTypeInfo.AggregationMethod == AggregationMethod.Min)
                res = (inputVal ?? 0) < aggVal ? inputVal : aggVal;
            else if (AggTypeInfo.AggregationMethod == AggregationMethod.Count)
                res = aggVal + 1;

            object output = Convert.ChangeType(
                res, TypeInfo.TryGetUnderlyingType(AggTypeInfo.AggregateColumnInOutput));
            AggTypeInfo.AggregateColumnInOutput.SetValue(aggOutput, output);
        }

        private decimal? ConvertToDecimal(object input)
        {
            if (input == null)
                return null;
            else
                return Convert.ToDecimal(input);
        }

        private void DefineStoreKeyActionFromAttributes(object key, TOutput outputRow)
        {
            var gk = key as GroupingKey;
            foreach (var propMap in gk?.GroupingObjectsByProperty)
                propMap.Key.SetValue(outputRow, propMap.Value);
        }

        private object DefineGroupingPropertyFromAttributes(TInput inputrow)
        {
            var gk = new GroupingKey();
            foreach (var propMap in AggTypeInfo.GroupColumnsInputAndOutput)
                gk?.GroupingObjectsByProperty.Add(propMap.Item2, propMap.Item1.GetValue(inputrow));
            return gk;
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
            object key = GroupingFunc?.Invoke(row) ?? string.Empty;

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

        class GroupingKey
        {
            public override int GetHashCode()
            {
                unchecked // Overflow is fine, just wrap
                {
                    int hash = 29;
                    foreach (var map in GroupingObjectsByProperty)
                        hash = hash * 486187739 + (map.Value?.GetHashCode() ?? 17);
                    return hash;
                }
            }
            public override bool Equals(object obj)
            {
                GroupingKey comp = obj as GroupingKey;
                if (comp == null) return false;
                bool equals = true;
                foreach (var map in GroupingObjectsByProperty)
                    equals &= (map.Value?.Equals(comp.GroupingObjectsByProperty[map.Key]) ?? true);
                return equals;
            }
            public Dictionary<PropertyInfo, object> GroupingObjectsByProperty { get; set; } = new Dictionary<PropertyInfo, object>();
        }
    }

    public enum AggregationMethod
    {
        Sum,
        Min,
        Max,
        Count
    }


}
