using ETLBox.DataFlow.Connectors;
using ETLBox.Exceptions;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// The lookup transformation enriches the incoming data with data from the lookup source.
    /// Data from the lookup source is read into memory when the first record arrives.
    /// For each incoming row, the lookup tries to find a matching record in the 
    /// loaded source data and uses this record to enrich the ingoing data.
    /// </summary>
    /// <example>
    /// <code>
    /// public class Order
    /// {    
    ///     public int OrderNumber { get; set; }
    ///     public int CustomerId { get; set; }
    ///     public string CustomerName { get; set; }
    /// }
    /// 
    /// public class Customer
    /// {
    ///     [RetrieveColumn(nameof(Order.CustomerId))]
    ///     public int Id { get; set; }
    /// 
    ///     [MatchColumn(nameof(Order.CustomerName))]
    ///     public string Name { get; set; }
    /// }
    /// 
    /// DbSource&lt;Order&gt; orderSource = new DbSource&lt;Order&gt;("OrderData");
    /// CsvSource&lt;Customer&gt; lookupSource = new CsvSource&lt;Customer&gt;("CustomerData.csv");
    /// var lookup = new LookupTransformation&lt;Order, Customer&gt;();
    /// lookup.Source = lookupSource;
    /// DbDestination&lt;Order&gt; dest = new DbDestination&lt;Order&gt;("OrderWithCustomerTable");
    /// source.LinkTo(lookup).LinkTo(dest);
    /// </code>
    /// </example>
    /// <typeparam name="TInput">Type of ingoing and outgoing data.</typeparam>
    /// <typeparam name="TSource">Type of data used in the lookup source.</typeparam>
    public class LookupTransformation<TInput, TSource> : DataFlowTransformation<TInput, TInput>
    where TSource : class
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = "Lookup";

        /// <summary>
        /// Holds the data read from the lookup source. This data is used to find data that is missing in the incoming rows.
        /// </summary>
        public ICollection<TSource> LookupData => CachedRowTransformation.CacheManager.Records;


        /// <inheritdoc/>
        public override ISourceBlock<TInput> SourceBlock => CachedRowTransformation.SourceBlock;

        /// <inheritdoc/>
        public override ITargetBlock<TInput> TargetBlock => CachedRowTransformation.TargetBlock;

        /// <summary>
        /// The source component from which the lookup data is retrieved. E.g. a <see cref="DbSource"/> or a <see cref="MemorySource"/>.
        /// </summary>
        public IDataFlowExecutableSource<TSource> Source { get; set; }

        /// <summary>
        /// A transformation func that describes how the ingoing data can be enriched with the already pre-read data from
        /// the <see cref="Source"/>
        /// </summary>
        public Func<TInput, ICollection<TSource>, TInput> TransformationFunc { get; set; }

        /// <summary>
        /// This collection will be used to define the matching columns - will also work with ExpandoObject.
        /// </summary>
        public IEnumerable<MatchColumn> MatchColumns { get; set; }

        /// <summary>
        /// This collection will be used to define the retrieve columns - will also work with ExpandoObject.
        /// </summary>
        public IEnumerable<RetrieveColumn> RetrieveColumns { get; set; }

        public class PartialDbCacheSettings
        {
            public Func<TInput, string> LoadCacheSql { get; set; }
            public Func<TInput,TSource,bool> MatchFunc { get; set; }
        }

        public CacheMode CacheMode { get; set; } = CacheMode.Full;
        public PartialDbCacheSettings PartialCacheSettings { get; set; } = new PartialDbCacheSettings();

        public new int ProgressCount => CachedRowTransformation.ProgressCount;        

        #endregion

        #region Constructors

        public LookupTransformation()
        {
            CachedRowTransformation = new CachedRowTransformation<TInput, TInput, TSource>();
        }

        /// <param name="lookupSource">Sets the <see cref="Source"/> of the lookup.</param>
        public LookupTransformation(IDataFlowExecutableSource<TSource> lookupSource) : this()
        {
            Source = lookupSource;
        }

        /// <param name="lookupSource">Sets the <see cref="Source"/> of the lookup.</param>
        /// <param name="transformationFunc">Sets the <see cref="TransformationFunc"/></param>
        public LookupTransformation(IDataFlowExecutableSource<TSource> lookupSource, Func<TInput, ICollection<TSource>, TInput> transformationFunc)
            : this(lookupSource)
        {
            TransformationFunc = transformationFunc;
        }

        #endregion

        #region Implement abstract methods

        protected override void InternalInitBufferObjects()
        {
            if (Source == null)
                throw new ETLBoxException("You need to define a lookup source before using a LookupTransformation in a data flow");
            if (TransformationFunc == null)
                DefaultFuncWithMatchRetrieveAttributes();

            InitRowTransformationManually();
        }


        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        internal override void CompleteBufferOnPredecessorCompletion() => CachedRowTransformation.CompleteBufferOnPredecessorCompletion();

        internal override void FaultBufferOnPredecessorCompletion(Exception e) => CachedRowTransformation.FaultBufferOnPredecessorCompletion(e);

        public new IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
        {
            var errorSource = InternalLinkErrorTo(target);
            CachedRowTransformation.ErrorSource = new ErrorSource() { Redirection = this.ErrorSource };
            Source.ErrorSource = new ErrorSource() { Redirection = this.ErrorSource };
            return errorSource;
        }

        #endregion

        #region Implementation

        CachedRowTransformation<TInput, TInput, TSource> CachedRowTransformation;
        LookupTypeInfo TypeInfo;
        List<AttributeMappingInfo> MatchAttributeMapping;
        List<AttributeMappingInfo> RetrieveAttributeMapping;

        private void InitRowTransformationManually()
        {
            CachedRowTransformation.TransformationFunc = TransformationFunc;
            CachedRowTransformation.CopyLogTaskProperties(this);
            CachedRowTransformation.MaxBufferSize = this.MaxBufferSize;
            if (CacheMode == CacheMode.PartialFromDb)
                CachedRowTransformation.CacheManager = new PartialDbTableCache<TInput, TSource>();
            else
                CachedRowTransformation.CacheManager = new FullTableCache<TInput, TSource>();
            (CachedRowTransformation.CacheManager as ILookupCacheManager<TInput, TSource>).Lookup = this;            
            CachedRowTransformation.InitBufferObjects();

        }

        private void DefaultFuncWithMatchRetrieveAttributes()
        {
            TypeInfo = new LookupTypeInfo(typeof(TInput), typeof(TSource));
            if (MatchColumns != null && RetrieveColumns != null)
                FillAttributeMappingFromProperties();
            else if (TypeInfo.MatchColumns.Count > 0 && TypeInfo.RetrieveColumns.Count > 0)
                FillAttributeMappingFromAttributes();
            else
                throw new ETLBoxException("Please define either a transformation function or use the MatchColumn / RetrieveColumn attributes.");
            TransformationFunc = FindRowByAttributes;
        }

        private void FillAttributeMappingFromProperties()
        {
            MatchAttributeMapping = new List<AttributeMappingInfo>();
            foreach (var mc in MatchColumns)
            {
                var newmap = new AttributeMappingInfo();
                newmap.PropNameInInput = mc.InputPropertyName;
                newmap.PropNameInOutput = mc.LookupSourcePropertyName;
                if (!TypeInfo.IsInputDynamic)
                    newmap.PropInInput = TypeInfo.InputPropertiesByName[mc.InputPropertyName];
                if (!TypeInfo.IsOutputDynamic)
                    newmap.PropInOutput = TypeInfo.OutputPropertiesByName[mc.LookupSourcePropertyName];
                MatchAttributeMapping.Add(newmap);
            }
            RetrieveAttributeMapping = new List<AttributeMappingInfo>();
            foreach (var rc in RetrieveColumns)
            {
                var newmap = new AttributeMappingInfo();
                newmap.PropNameInInput = rc.InputPropertyName;
                newmap.PropNameInOutput = rc.LookupSourcePropertyName;
                if (!TypeInfo.IsInputDynamic)
                    newmap.PropInInput = TypeInfo.InputPropertiesByName[rc.InputPropertyName];
                if (!TypeInfo.IsOutputDynamic)
                    newmap.PropInOutput = TypeInfo.OutputPropertiesByName[rc.LookupSourcePropertyName];
                RetrieveAttributeMapping.Add(newmap);
            }
        }

        private void FillAttributeMappingFromAttributes()
        {
            MatchAttributeMapping = TypeInfo.MatchColumns;
            RetrieveAttributeMapping = TypeInfo.RetrieveColumns;
        }

        private TInput FindRowByAttributes(TInput row, ICollection<TSource> cache)
        {
            var lookupHit = cache.FindFirst(e =>
            {
                bool same = true;
                foreach (var mc in MatchAttributeMapping)
                {
                    var inputValue = GetInputValue(row, mc);
                    var outputValue = GetLookupSourceValue(e, mc);
                    if (inputValue == null && outputValue == null)
                        same = true;
                    else if ((inputValue != null && outputValue == null)
                            || (inputValue == null && outputValue != null))
                        same = false;
                    else
                        same &= inputValue.Equals(outputValue);
                    if (!same) break;
                }
                return same;
            });
            if (lookupHit != null)
            {
                foreach (var rc in RetrieveAttributeMapping)
                {
                    var retrieveValue = GetLookupSourceValue(lookupHit, rc);
                    TrySetValueInInput(row, rc, retrieveValue);
                }
            }
            return row;
        }               

        private object GetInputValue(TInput row, AttributeMappingInfo mc)
        {
            if (TypeInfo.IsInputDynamic)
            {
                IDictionary<string, object> dict = row as IDictionary<string, object>;
                return dict.ContainsKey(mc.PropNameInInput) ? dict[mc.PropNameInInput] : null;
            }
            else  
                return mc.PropInInput.GetValue(row);
        }

        private object GetLookupSourceValue(TSource e, AttributeMappingInfo mc)
        {
            if (TypeInfo.IsOutputDynamic)
            {
                IDictionary<string, object> dict = e as IDictionary<string, object>;
                return dict.ContainsKey(mc.PropNameInOutput) ? dict[mc.PropNameInOutput] : null;                
            }
            else 
                return mc.PropInOutput.GetValue(e);
        }

        private void TrySetValueInInput(TInput row, AttributeMappingInfo rc, object retrieveValue)
        {
            if (TypeInfo.IsInputDynamic)
            {
                IDictionary<string, object> dict = row as IDictionary<string, object>;
                if (dict.ContainsKey(rc.PropNameInInput))
                    dict[rc.PropNameInInput] = retrieveValue;
                else
                    dict.Add(rc.PropNameInInput, retrieveValue);
            }
            else 
                rc.PropInInput.TrySetValue(row, retrieveValue);
        }

        #endregion

    }

    /// <inheritdoc/>
    public class LookupTransformation : LookupTransformation<ExpandoObject, ExpandoObject>
    {
        public LookupTransformation() : base()
        { }

        public LookupTransformation(IDataFlowExecutableSource<ExpandoObject> lookupSource)
            : base(lookupSource)
        { }

        public LookupTransformation(IDataFlowExecutableSource<ExpandoObject> lookupSource, Func<ExpandoObject, ICollection<ExpandoObject>, ExpandoObject> transformationFunc)
            : base(lookupSource, transformationFunc)
        { }
    }

    public enum CacheMode
    {
        Full,
        PartialFromDb
    }

}
