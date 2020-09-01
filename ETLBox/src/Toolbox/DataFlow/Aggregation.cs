using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Reflection;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// Aggregates data by the given aggregation methods.
    /// The aggregate is a partial-blocking transformation - only the aggregation values are stored in separate memory objects.
    /// When all rows have been processed by the aggregation, the aggregated values are written into the output.
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data.</typeparam>
    /// <typeparam name="TOutput">Type of outgoing data.</typeparam>
    public class Aggregation<TInput, TOutput> : DataFlowTransformation<TInput, TOutput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = "Execute aggregation block";

        /// <summary>
        /// This action describes how the input data is aggregated
        /// </summary>
        public Action<TInput, TOutput> AggregationAction { get; set; }

        /// <summary>
        /// This Func defines the aggregation level for the input data
        /// </summary>
        public Func<TInput, object> GroupingFunc { get; set; }

        /// <summary>
        /// This action will store the result of the aggregation from the input in the output object
        /// </summary>
        public Action<object, TOutput> StoreKeyAction { get; set; }

        /// <inheritdoc/>
        public override ISourceBlock<TOutput> SourceBlock => OutputBuffer;

        /// <inheritdoc/>
        public override ITargetBlock<TInput> TargetBlock => InputBuffer;

        #endregion

        #region Constructors

        public Aggregation()
        {
            AggTypeInfo = new AggregationTypeInfo(typeof(TInput), typeof(TOutput));
            CheckTypeInfo();
        }

        /// <param name="aggregationAction">Sets the <see cref="AggregationAction"/></param>
        public Aggregation(Action<TInput, TOutput> aggregationAction) : this()
        {
            AggregationAction = aggregationAction;
        }

        /// <param name="aggregationAction">Sets the <see cref="AggregationAction"/></param>
        /// <param name="groupingFunc">Sets the <see cref="GroupingFunc"/></param>
        public Aggregation(Action<TInput, TOutput> aggregationAction, Func<TInput, object> groupingFunc)
            : this(aggregationAction)
        {
            GroupingFunc = groupingFunc;
        }

        /// <param name="aggregationAction">Sets the <see cref="AggregationAction"/></param>
        /// <param name="groupingFunc">Sets the <see cref="GroupingFunc"/></param>
        /// <param name="storeKeyAction">Sets the <see cref="StoreKeyAction"/></param>
        public Aggregation(Action<TInput, TOutput> aggregationAction, Func<TInput, object> groupingFunc, Action<object, TOutput> storeKeyAction)
            : this(aggregationAction, groupingFunc)
        {
            StoreKeyAction = storeKeyAction;
        }

        #endregion

        #region Implement abstract methods

        internal override Task BufferCompletion => SourceBlock.Completion;

        protected override void InternalInitBufferObjects()
        {
            SetAggregationFunctionsIfNecessary();

            OutputBuffer = new BufferBlock<TOutput>(new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
            InputBuffer = new ActionBlock<TInput>(WrapAggregationAction, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize,
            });
            InputBuffer.Completion.ContinueWith(t =>
            {
                if (t.IsFaulted) ((IDataflowBlock)OutputBuffer).Fault(t.Exception.InnerException);
                try
                {
                    NLogStartOnce();
                    WriteIntoOutput();
                    OutputBuffer.Complete();
                }
                catch (Exception e)
                {
                    ((IDataflowBlock)OutputBuffer).Fault(e);
                    throw e;
                }
            });
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        internal override void CompleteBufferOnPredecessorCompletion() => TargetBlock.Complete();

        internal override void FaultBufferOnPredecessorCompletion(Exception e) => TargetBlock.Fault(e);

        #endregion

        #region Implementation

        BufferBlock<TOutput> OutputBuffer;
        ActionBlock<TInput> InputBuffer;
        Dictionary<object, TOutput> AggregationData = new Dictionary<object, TOutput>();
        AggregationTypeInfo AggTypeInfo;

        private void CheckTypeInfo()
        {
            if (AggTypeInfo.IsArrayOutput)
                throw new Exception("Aggregation target must be of an object or dynamic type! Array types are not allowed.");
        }

        private void SetAggregationFunctionsIfNecessary()
        {
            if (AggregationAction == null && AggTypeInfo.AggregateColumns.Count > 0)
                AggregationAction = DefineAggregationAction;

            if (GroupingFunc == null && AggTypeInfo.GroupColumns.Count > 0)
                GroupingFunc = DefineGroupingPropertyFromAttributes;

            if (StoreKeyAction == null && AggTypeInfo.GroupColumns.Count > 0)
                StoreKeyAction = DefineStoreKeyActionFromAttributes;
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
                    res = ((inputVal ?? 0) > aggVal) ? inputVal : aggVal;
                else if (attrmap.AggregationMethod == AggregationMethod.Min)
                    res = (inputVal ?? 0) < aggVal ? inputVal : aggVal;
                else if (attrmap.AggregationMethod == AggregationMethod.Count)
                    res = aggVal + 1;

                object output = Convert.ChangeType(
                    res, TypeInfo.TryGetUnderlyingType(attrmap.PropInOutput));
                attrmap.PropInOutput.SetValueOrThrow(aggOutput, output);
            }
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
                propMap.Key.TrySetValue(outputRow, propMap.Value);
        }

        private object DefineGroupingPropertyFromAttributes(TInput inputrow)
        {
            var gk = new GroupingKey();
            foreach (var propMap in AggTypeInfo.GroupColumns)
                gk?.GroupingObjectsByProperty.Add(propMap.PropInOutput, propMap.PropInInput.GetValue(inputrow));
            return gk;
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
            TOutput firstEntry = default;
            firstEntry = (TOutput)Activator.CreateInstance(typeof(TOutput));
            AggregationData.Add(key, firstEntry);
        }

        private void WriteIntoOutput()
        {
            foreach (var row in AggregationData)
            {
                StoreKeyAction?.Invoke(row.Key, row.Value);
                if (!OutputBuffer.SendAsync(row.Value).Result)
                    throw new ETLBoxException("Buffer already completed or faulted!", this.Exception);
                LogProgress();
            }
        }

        class GroupingKey
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
                if (comp == null) return false;
                bool equals = true;
                foreach (var map in GroupingObjectsByProperty)
                    equals &= (map.Value?.Equals(comp.GroupingObjectsByProperty[map.Key]) ?? true);
                return equals;
            }
            public Dictionary<PropertyInfo, object> GroupingObjectsByProperty { get; set; } = new Dictionary<PropertyInfo, object>();
        }

        #endregion
    }

    public enum AggregationMethod
    {
        Sum,
        Min,
        Max,
        Count
    }

    /// <inheritdoc />
    public class Aggregation : Aggregation<ExpandoObject, ExpandoObject>
    {
        public Aggregation(Action<ExpandoObject, ExpandoObject> aggregationAction) : base(aggregationAction)
        { }

        public Aggregation(Action<ExpandoObject, ExpandoObject> aggregationAction, Func<ExpandoObject, object> groupingFunc)
            : base(aggregationAction, groupingFunc)
        { }

        public Aggregation(Action<ExpandoObject, ExpandoObject> aggregationAction, Func<ExpandoObject, object> groupingFunc, Action<object, ExpandoObject> storeKeyAction)
            : base(aggregationAction, groupingFunc, storeKeyAction)
        { }
    }
}
