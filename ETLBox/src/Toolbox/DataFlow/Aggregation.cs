using System.Threading.Tasks.Dataflow;
using ALE.ETLBox.Helper;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Aggregates data by the given aggregation method.
    /// </summary>
    /// <typeparam name="TInput">Type of data input</typeparam>
    /// <typeparam name="TOutput">Type of data output</typeparam>
    [PublicAPI]
    public class Aggregation<TInput, TOutput> : DataFlowTransformation<TInput, TOutput>
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = "Execute aggregation block.";

        /* Public Properties */
        public Action<TInput, TOutput> AggregationAction
        {
            get { return _aggregationAction; }
            set
            {
                _aggregationAction = value;
                InputBuffer = new ActionBlock<TInput>(WrapAggregationAction);
                InputBuffer.Completion.ContinueWith(t =>
                {
                    if (t.IsFaulted)
                        ((IDataflowBlock)OutputBuffer).Fault(t.Exception!.InnerException!);
                    try
                    {
                        WriteIntoOutput();
                        OutputBuffer.Complete();
                    }
                    catch (Exception e)
                    {
                        ((IDataflowBlock)OutputBuffer).Fault(e);
                        throw;
                    }
                });
            }
        }
        public Func<TInput, object> GroupingFunc { get; set; }
        public Action<object, TOutput> StoreKeyAction { get; set; }
        public override ISourceBlock<TOutput> SourceBlock => OutputBuffer;
        public override ITargetBlock<TInput> TargetBlock => InputBuffer;

        /* Private stuff */
        private BufferBlock<TOutput> OutputBuffer { get; set; }
        private ActionBlock<TInput> InputBuffer { get; set; }

        private Action<TInput, TOutput> _aggregationAction;
        private Dictionary<object, TOutput> AggregationData { get; set; } = new();
        private AggregationTypeInfo AggTypeInfo { get; set; }

        public Aggregation()
        {
            OutputBuffer = new BufferBlock<TOutput>();
            AggTypeInfo = new AggregationTypeInfo(typeof(TInput), typeof(TOutput));

            CheckTypeInfo();

            if (AggregationAction == null && AggTypeInfo.AggregateColumns.Count > 0)
                AggregationAction = DefineAggregationAction;

            if (GroupingFunc == null && AggTypeInfo.GroupColumns.Count > 0)
                GroupingFunc = DefineGroupingPropertyFromAttributes;

            if (StoreKeyAction == null && AggTypeInfo.GroupColumns.Count > 0)
                StoreKeyAction = DefineStoreKeyActionFromAttributes;
        }

        private void CheckTypeInfo()
        {
            if (AggTypeInfo.IsArrayOutput)
                throw new Exception(
                    "Aggregation target must be of an object or dynamic type! Array types are not allowed."
                );
        }

        public Aggregation(Action<TInput, TOutput> aggregationAction)
            : this()
        {
            AggregationAction = aggregationAction;
        }

        public Aggregation(
            Action<TInput, TOutput> aggregationAction,
            Func<TInput, object> groupingFunc
        )
            : this(aggregationAction)
        {
            GroupingFunc = groupingFunc;
        }

        public Aggregation(
            Action<TInput, TOutput> aggregationAction,
            Func<TInput, object> groupingFunc,
            Action<object, TOutput> storeKeyAction
        )
            : this(aggregationAction, groupingFunc)
        {
            StoreKeyAction = storeKeyAction;
        }

        private void DefineAggregationAction(TInput inputrow, TOutput aggOutput)
        {
            foreach (var attrmap in AggTypeInfo.AggregateColumns)
            {
                decimal? inputVal = ConvertToDecimal(attrmap.PropInInput.GetValue(inputrow));
                decimal? aggVal = ConvertToDecimal(attrmap.PropInOutput.GetValue(aggOutput));
                decimal? res = null;
                if (aggVal == null && attrmap.AggregationMethod == AggregationMethod.Count)
                    res = 1;
                else if (aggVal == null)
                    res = inputVal;
                else if (attrmap.AggregationMethod == AggregationMethod.Sum)
                    res = (inputVal ?? 0) + aggVal;
                else if (attrmap.AggregationMethod == AggregationMethod.Max)
                    res = (inputVal ?? 0) > aggVal ? inputVal : aggVal;
                else if (attrmap.AggregationMethod == AggregationMethod.Min)
                    res = (inputVal ?? 0) < aggVal ? inputVal : aggVal;
                else if (attrmap.AggregationMethod == AggregationMethod.Count)
                    res = aggVal + 1;

                object output = Convert.ChangeType(
                    res,
                    TypeInfo.TryGetUnderlyingType(attrmap.PropInOutput)
                );
                attrmap.PropInOutput.SetValueOrThrow(aggOutput, output);
            }
        }

        private decimal? ConvertToDecimal(object input)
        {
            if (input == null)
                return null;
            return Convert.ToDecimal(input);
        }

        private void DefineStoreKeyActionFromAttributes(object key, TOutput outputRow)
        {
            var gk = key as GroupingKey;
            if (gk == null)
                return;
            foreach (var propMap in gk.GroupingObjectsByProperty)
                propMap.Key.TrySetValue(outputRow, propMap.Value);
        }

        private object DefineGroupingPropertyFromAttributes(TInput inputrow)
        {
            var gk = new GroupingKey();
            foreach (var propMap in AggTypeInfo.GroupColumns)
                gk.GroupingObjectsByProperty.Add(
                    propMap.PropInOutput,
                    propMap.PropInInput.GetValue(inputrow)
                );
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
            var firstEntry = (TOutput)Activator.CreateInstance(typeof(TOutput));
            AggregationData.Add(key, firstEntry);
        }

        private class GroupingKey
        {
            public override int GetHashCode()
            {
                unchecked
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
                if (comp == null)
                    return false;
                bool equals = true;
                foreach (var map in GroupingObjectsByProperty)
                    equals &= map.Value?.Equals(comp.GroupingObjectsByProperty[map.Key]) ?? true;
                return equals;
            }

            public Dictionary<PropertyInfo, object> GroupingObjectsByProperty { get; } = new();
        }
    }

    public enum AggregationMethod
    {
        Sum,
        Min,
        Max,
        Count
    }

    /// <summary>
    /// Aggregates data by the given aggregation method.
    /// The non generic implementation uses dynamic objects.
    /// </summary>
    /// <see cref="Aggregation{TInput, TOutput}"/>
    [PublicAPI]
    public class Aggregation : Aggregation<ExpandoObject, ExpandoObject>
    {
        public Aggregation(Action<ExpandoObject, ExpandoObject> aggregationAction)
            : base(aggregationAction) { }

        public Aggregation(
            Action<ExpandoObject, ExpandoObject> aggregationAction,
            Func<ExpandoObject, object> groupingFunc
        )
            : base(aggregationAction, groupingFunc) { }

        public Aggregation(
            Action<ExpandoObject, ExpandoObject> aggregationAction,
            Func<ExpandoObject, object> groupingFunc,
            Action<object, ExpandoObject> storeKeyAction
        )
            : base(aggregationAction, groupingFunc, storeKeyAction) { }
    }
}
