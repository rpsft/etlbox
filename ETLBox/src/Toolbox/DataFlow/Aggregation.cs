using System.Linq;
using ALE.ETLBox.Common;
using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.Helper;
using TypeInfo = ALE.ETLBox.Common.DataFlow.TypeInfo;

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

        private void CheckTypeInfo()
        {
            if (AggTypeInfo.IsArrayOutput)
                throw new ETLBoxException(
                    "Aggregation target must be of an object or dynamic type! Array types are not allowed."
                );
        }

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

        private void DefineAggregationAction(TInput inputRow, TOutput aggOutput)
        {
            foreach (var attributeMapping in AggTypeInfo.AggregateColumns)
            {
                var inputVal = ConvertToDecimal(
                    attributeMapping.PropInInput.GetValue(inputRow)
                );
                var aggVal = ConvertToDecimal(
                    attributeMapping.PropInOutput.GetValue(aggOutput)
                );
                var res = (aggVal, attributeMapping.AggregationMethod) switch
                {
                    (null, AggregationMethod.Count) => 1,
                    (null, _) => inputVal,
                    (_, AggregationMethod.Sum) => (inputVal ?? 0) + aggVal,
                    (_, AggregationMethod.Max) => (inputVal ?? 0) > aggVal ? inputVal : aggVal,
                    (_, AggregationMethod.Min) => (inputVal ?? 0) < aggVal ? inputVal : aggVal,
                    (_, AggregationMethod.Count) => aggVal + 1,
                    (_, _) => null
                };

                var output = Convert.ChangeType(
                    res,
                    TypeInfo.TryGetUnderlyingType(attributeMapping.PropInOutput)
                );
                attributeMapping.PropInOutput.SetValueOrThrow(aggOutput, output);
            }
        }

        private static decimal? ConvertToDecimal(object input)
        {
            if (input == null)
                return null;
            return Convert.ToDecimal(input);
        }

        private void DefineStoreKeyActionFromAttributes(object key, TOutput outputRow)
        {
            if (key is not GroupingKey gk)
                return;
            foreach (var propMap in gk.GroupingObjectsByProperty)
                propMap.Key.TrySetValue(outputRow, propMap.Value);
        }

        private GroupingKey DefineGroupingPropertyFromAttributes(TInput inputRow)
        {
            var groupingKey = new GroupingKey();
            foreach (var propMap in AggTypeInfo.GroupColumns)
                groupingKey.GroupingObjectsByProperty.Add(
                    propMap.PropInOutput,
                    propMap.PropInInput.GetValue(inputRow)
                );
            return groupingKey;
        }

        private void WriteIntoOutput()
        {
            LogStart();
            foreach (var row in AggregationData)
            {
                StoreKeyAction?.Invoke(row.Key, row.Value);
                OutputBuffer.SendAsync(row.Value).Wait();
                LogProgress();
            }
            LogFinish();
        }

        private void WrapAggregationAction(TInput row)
        {
            var key = GroupingFunc?.Invoke(row) ?? string.Empty;

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

        private sealed class GroupingKey
        {
            public override int GetHashCode()
            {
                unchecked
                {
                    var hash = 29;
                    foreach (var map in GroupingObjectsByProperty)
                        hash = hash * 486187739 + (map.Value?.GetHashCode() ?? 17);
                    return hash;
                }
            }

            public override bool Equals(object obj) =>
                obj is GroupingKey comp
                && GroupingObjectsByProperty.All(
                    map => map.Value?.Equals(comp.GroupingObjectsByProperty[map.Key]) ?? true
                );

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
