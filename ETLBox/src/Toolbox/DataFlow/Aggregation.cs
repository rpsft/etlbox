using ETLBox.Exceptions;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// Aggregates data by the given aggregation methods.
    /// The aggregate is a partial-blocking transformation - only the aggregation values are stored in separate memory objects.
    /// When all rows have been processed by the aggregation, the aggregated values are written into the output.
    /// </summary>
    /// <example>
    /// <code>
    /// public class MyDetailValue
    /// {
    ///     public int DetailValue { get; set; }
    /// }
    /// 
    /// public class MyAggRow
    /// {
    ///     [AggregateColumn(nameof(MyDetailValue.DetailValue), AggregationMethod.Sum)]
    ///     public int AggValue { get; set; }
    /// }
    /// 
    /// var source = new DbSource&lt;MyDetailValue&gt;("DetailValues");
    /// var agg = new Aggregation&lt;MyDetailValue, MyAggRow&gt;();
    /// var dest = new MemoryDestination&lt;MyAggRow&gt;();
    /// source.LinkTo&lt;MyAggRow&gt;(agg).LinkTo(dest);
    /// </code>
    /// </example>
    /// <typeparam name="TInput">Type of ingoing data.</typeparam>
    /// <typeparam name="TOutput">Type of outgoing data.</typeparam>
    public class Aggregation<TInput, TOutput> : DataFlowTransformation<TInput, TOutput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = "Execute aggregation block";

        /// <summary>
        /// This action describes how the input data is aggregated.
        /// Not needed if you use the <see cref="AggregateColumn" /> in your object
        /// or pass a list to the <see cref="AggregateColumns" /> property. 
        /// </summary>
        public Action<TInput, TOutput> AggregationAction { get; set; }

        /// <summary>
        /// This Func defines how the object for grouping data is retrieved.
        /// Not needed if you use the <see cref="GroupColumn" /> in your object
        /// or pass a list to the <see cref="GroupColumns" /> property. 
        /// </summary>
        public Func<TInput, object> GroupingFunc { get; set; }

        /// <summary>
        /// This action defines how the grouping object is written back into the aggregated object.
        /// Not needed if you use the <see cref="GroupColumn" /> in your object
        /// or pass a list to the <see cref="GroupColumns" /> property. 
        /// </summary>
        public Action<object, TOutput> StoreKeyAction { get; set; }

        /// <inheritdoc/>
        public override ISourceBlock<TOutput> SourceBlock => OutputBuffer;

        /// <inheritdoc/>
        public override ITargetBlock<TInput> TargetBlock => InputBuffer;

        /// <summary>
        /// This list will be used to set the AggregationAction.
        /// This also works with ExpandoObjects.
        /// </summary>
        public IEnumerable<AggregateColumn> AggregateColumns { get; set; }

        /// <summary>
        /// This list will be used to set the <see cref="GroupingFunc"/> and the <see cref="StoreKeyAction" />.
        /// This also works with ExpandoObjects.
        /// </summary>
        public IEnumerable<GroupColumn> GroupColumns { get; set; }

        #endregion

        #region Constructors

        public Aggregation()
        {
            AggTypeInfo = new AggregationTypeInfo(typeof(TInput), typeof(TOutput));
            InputTypeInfo = new TypeInfo(typeof(TInput)).GatherTypeInfo();
            OutputTypeInfo = new TypeInfo(typeof(TOutput)).GatherTypeInfo();
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
            CheckIfAggregationActionIsSet();

            OutputBuffer = new BufferBlock<TOutput>(new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
            InputBuffer = new ActionBlock<TInput>(WrapAggregationAction, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = -1 //Always -1, as this is a blocking transformation
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

        internal override void CompleteBuffer() => TargetBlock.Complete();

        internal override void FaultBuffer(Exception e) { 
            TargetBlock.Fault(e);
            SourceBlock.Fault(e);
        }
        #endregion

        #region Implementation

        BufferBlock<TOutput> OutputBuffer;
        ActionBlock<TInput> InputBuffer;
        Dictionary<object, TOutput> AggregationData = new Dictionary<object, TOutput>();
        AggregationTypeInfo AggTypeInfo;
        TypeInfo InputTypeInfo;
        TypeInfo OutputTypeInfo;
        List<AggregateAttributeMapping> AggregateAttributeMapping;
        List<AttributeMappingInfo> GroupingAttributeMapping;

        private void CheckTypeInfo()
        {
            if (AggTypeInfo.IsArrayOutput)
                throw new Exception("Aggregation target must be of an object or dynamic type! Array types are not allowed.");
        }

        private void SetAggregationFunctionsIfNecessary()
        {
            if (AggregationAction == null && (AggTypeInfo.AggregateColumns.Count > 0 || AggregateColumns?.Count() > 0))
            {
                FillAggregateAttributeMapping();
                AggregationAction = DefineAggregationAction;
            }

            if (GroupingFunc == null && (AggTypeInfo.GroupColumns.Count > 0 || GroupColumns?.Count() > 0))
            {
                FillGroupingAttributeMapping();
                GroupingFunc = DefineGroupingProperty;
                if (StoreKeyAction == null)
                    StoreKeyAction = DefineStoreKeyAction;
            }

        }

        private void CheckIfAggregationActionIsSet()
        {
            if (AggregationAction == null)
                throw new ETLBoxException("No aggregation method found - either define an AggregationAction or use the AggregateColumn attribute");
        }

        private void FillAggregateAttributeMapping()
        {
            if (AggregateColumns != null)
            {
                AggregateAttributeMapping = new List<AggregateAttributeMapping>();
                foreach (var ac in AggregateColumns)
                {
                    var newMap = new AggregateAttributeMapping();
                    newMap.PropNameInInput = ac.InputProperty;
                    newMap.PropNameInOutput = ac.AggregationProperty;
                    newMap.AggregationMethod = ac.AggregationMethod;
                    if (!InputTypeInfo.IsDynamic)
                        newMap.PropInInput = InputTypeInfo.PropertiesByName[ac.InputProperty];
                    if (!OutputTypeInfo.IsDynamic)
                    {
                        newMap.PropInOutput = OutputTypeInfo.PropertiesByName[ac.AggregationProperty];
                        newMap.OutputType = TypeInfo.TryGetUnderlyingType(OutputTypeInfo.PropertiesByName[ac.AggregationProperty]);
                    }

                    AggregateAttributeMapping.Add(newMap);
                }
            }
            else
            {
                AggregateAttributeMapping = AggTypeInfo.AggregateColumns;
            }
        }

        private void DefineAggregationAction(TInput inputrow, TOutput aggOutput)
        {
            foreach (var attrmap in AggregateAttributeMapping)
            {
                object inputVal = GetValueFromInputObject(inputrow, attrmap);
                object aggVal = GetValueFromOutputObject(aggOutput, attrmap);
                object res = null;

                if (attrmap.AggregationMethod == AggregationMethod.Count)
                    res = Count(aggVal);
                else if (attrmap.AggregationMethod == AggregationMethod.Sum)
                    res = Sum(inputVal, aggVal);
                else if (attrmap.AggregationMethod == AggregationMethod.Max)
                    res = Max(inputVal, aggVal);
                else if (attrmap.AggregationMethod == AggregationMethod.Min)
                    res = Min(inputVal, aggVal);
                else if (attrmap.AggregationMethod == AggregationMethod.FirstNotNullValue)
                    res = FirstNotNullValue(inputVal, aggVal);
                else if (attrmap.AggregationMethod == AggregationMethod.LastNotNullValue)
                    res = LastNotNullValue(inputVal, aggVal);
                else if (attrmap.AggregationMethod == AggregationMethod.FirstValue)
                    res = FirstValue(inputVal, aggVal);
                else if (attrmap.AggregationMethod == AggregationMethod.LastValue)
                    res = LastValue(inputVal, aggVal);

                if (OutputTypeInfo.IsDynamic)
                    SetValueInOutputObject(aggOutput, attrmap, res);
                else
                    SetValueInOutputObject(aggOutput, attrmap,
                        TryConvert(attrmap.OutputType, res)
                    );
            }
        }

        private int? Count(object aggVal)
        {
            int? aggValAsInt = TryConvertToInt(aggVal);
            if (aggValAsInt == null)
                return 1;
            else
                return aggValAsInt + 1;
        }

        private decimal? Sum(object inputVal, object aggVal)
        {
            decimal? inputValAsDec = TryConvertToDecimal(inputVal);
            decimal? aggValAsDec = TryConvertToDecimal(aggVal);
            if (aggValAsDec == null)
                return inputValAsDec;
            else
                return (inputValAsDec ?? 0) + aggValAsDec;
        }

        private decimal? Min(object inputVal, object aggVal)
        {
            decimal? inputValAsDec = TryConvertToDecimal(inputVal);
            decimal? aggValAsDec = TryConvertToDecimal(aggVal);
            if (aggValAsDec == null)
                return inputValAsDec;
            else
                return (inputValAsDec ?? 0) < aggValAsDec ? inputValAsDec : aggValAsDec;
        }

        private decimal? Max(object inputVal, object aggVal)
        {
            decimal? inputValAsDec = TryConvertToDecimal(inputVal);
            decimal? aggValAsDec = TryConvertToDecimal(aggVal);
            if (aggValAsDec == null)
                return inputValAsDec;
            else
                return ((inputValAsDec ?? 0) > aggValAsDec) ? inputValAsDec : aggValAsDec;
        }

        private object FirstNotNullValue(object inputVal, object aggVal)
        {
            if (aggVal == null)
                return inputVal;
            else
                return aggVal;
        }

        private object LastNotNullValue(object inputVal, object aggVal)
        {
            if (inputVal != null)
                return inputVal;
            else
                return aggVal;
        }

        bool IsFirstRowProcessed;
        private object FirstValue(object inputVal, object aggVal)
        {
            if (!IsFirstRowProcessed)
            {
                IsFirstRowProcessed = true;
                return inputVal;
            }
            else
                return aggVal;
        }

        private object LastValue(object inputVal, object aggVal)
        {
            return inputVal;
        }

        private decimal? TryConvertToDecimal(object input) => input == null ? (decimal?)null : Convert.ToDecimal(input);

        private int? TryConvertToInt(object input) => input == null ? (int?)null : Convert.ToInt32(input);

        private object TryConvert(Type outputType, object res)
            => res != null ? Convert.ChangeType(res, outputType) : null;

        private object GetValueFromInputObject(TInput inputrow, AttributeMappingInfo attrmap)
        {
            if (InputTypeInfo.IsDynamic)
            {
                IDictionary<string, object> dict = inputrow as IDictionary<string, object>;
                return dict.ContainsKey(attrmap.PropNameInInput) ? dict[attrmap.PropNameInInput] : null; 
            }
            else
                return attrmap.PropInInput.GetValue(inputrow);
        }

        private object GetValueFromOutputObject(TOutput aggOutput, AttributeMappingInfo attrmap)
        {
            if (OutputTypeInfo.IsDynamic)
            {
                IDictionary<string, object> dict = aggOutput as IDictionary<string, object>;
                return dict.ContainsKey(attrmap.PropNameInOutput) ? dict[attrmap.PropNameInOutput] : null;
            }
            else
                return attrmap.PropInOutput.GetValue(aggOutput);
        }

        private void SetValueInOutputObject(TOutput aggOutput, AttributeMappingInfo attrmap, object output)
        {
            if (OutputTypeInfo.IsDynamic)
            {
                IDictionary<string, object> dict = aggOutput as IDictionary<string, object>;
                if (dict.ContainsKey(attrmap.PropNameInOutput))
                    dict[attrmap.PropNameInOutput] = output;
                else
                    dict.Add(attrmap.PropNameInOutput, output);
            }
            else
                attrmap.PropInOutput.SetValueOrThrow(aggOutput, output);
        }


        private void FillGroupingAttributeMapping()
        {
            if (GroupColumns != null)
            {
                GroupingAttributeMapping = new List<AttributeMappingInfo>();
                foreach (var gc in GroupColumns)
                {
                    var newMap = new AttributeMappingInfo();
                    newMap.PropNameInInput = gc.InputGroupingProperty;
                    newMap.PropNameInOutput = gc.OutputGroupingProperty;

                    if (!InputTypeInfo.IsDynamic)
                        newMap.PropInInput = InputTypeInfo.PropertiesByName[gc.InputGroupingProperty];
                    if (!OutputTypeInfo.IsDynamic)
                        newMap.PropInOutput = OutputTypeInfo.PropertiesByName[gc.OutputGroupingProperty];

                    GroupingAttributeMapping.Add(newMap);
                }
            }
            else
            {
                GroupingAttributeMapping = AggTypeInfo.GroupColumns;
            }
        }

        private object DefineGroupingProperty(TInput inputrow)
        {
            var gk = new GroupingKey();
            foreach (var map in GroupingAttributeMapping)
                gk?.GroupingObjectsByProperty.Add(map.PropNameInInput, GetValueFromInputObject(inputrow, map));
            return gk;
        }

        private void DefineStoreKeyAction(object key, TOutput outputRow)
        {
            var gk = key as GroupingKey;
            foreach (var go in gk?.GroupingObjectsByProperty)
            {
                AttributeMappingInfo map = GroupingAttributeMapping.Find(m => m.PropNameInInput == go.Key);
                SetValueInOutputObject(outputRow, map, go.Value);
            }
        }

        private void WrapAggregationAction(TInput row)
        {
            try
            {
                object key = GroupingFunc?.Invoke(row) ?? string.Empty;

                if (!AggregationData.ContainsKey(key))
                    AddRecordToDict(key);

                TOutput currentAgg = AggregationData[key];
                AggregationAction.Invoke(row, currentAgg);
            }
            catch (Exception e)
            {
                ThrowOrRedirectError(e, ErrorSource.ConvertErrorData<TInput>(row));
                return;
            }
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
            public override int GetHashCode() => HashHelper.HashSum(GroupingObjectsByProperty.Values);

            public override bool Equals(object obj)
            {
                GroupingKey comp = obj as GroupingKey;
                if (comp == null) return false;
                bool equals = true;
                foreach (var map in GroupingObjectsByProperty)
                    equals &= (map.Value?.Equals(comp.GroupingObjectsByProperty[map.Key]) ?? true);
                return equals;
            }
            public Dictionary<string, object> GroupingObjectsByProperty { get; set; } = new Dictionary<string, object>();
        }

        #endregion
    }

    public enum AggregationMethod
    {
        Sum,
        Min,
        Max,
        Count,
        FirstNotNullValue,
        LastNotNullValue,
        FirstValue,
        LastValue
    }

    /// <inheritdoc />
    public class Aggregation : Aggregation<ExpandoObject, ExpandoObject>
    {
        public Aggregation() : base() { }

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
