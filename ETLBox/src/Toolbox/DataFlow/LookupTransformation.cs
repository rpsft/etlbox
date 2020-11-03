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
    /// memory table and uses this record to add additional data to the ingoing row. 
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

        public new int ProgressCount
        {
            get
            {
                return CachedRowTransformation.ProgressCount;
            }
            set
            {
                CachedRowTransformation.ProgressCount = value;
            }
        }

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
            if (TransformationFunc == null)
                DefaultFuncWithMatchRetrieveAttributes();
            InitRowTransformationManually();

            if (Source == null) throw new ETLBoxException("You need to define a lookup source before using a LookupTransformation in a data flow");
            var cm = CachedRowTransformation.CacheManager as FullTableCache<TInput, TSource>;
            cm.Source = Source;
            cm.Lookup = this;
            //cm.TableName = "LookupSameType";
            //cm.connMan = (Source as DbSource<TSource>).ConnectionManager;
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

        private void InitRowTransformationManually()
        {
            CachedRowTransformation.TransformationFunc = TransformationFunc;
            CachedRowTransformation.CopyLogTaskProperties(this);
            //CachedRowTransformation.InitAction = initAction;
            CachedRowTransformation.MaxBufferSize = this.MaxBufferSize;
            CachedRowTransformation.CacheManager = new FullTableCache<TInput, TSource>();
            CachedRowTransformation.InitBufferObjects();
            
        }

        //private void LinkInternalLoadBufferFlow()
        //{

        //    //LookupBuffer.CopyLogTaskProperties(this);
        //    //Source.LinkTo(LookupBuffer);
        //}

        private void DefaultFuncWithMatchRetrieveAttributes()
        {
            TypeInfo = new LookupTypeInfo(typeof(TInput), typeof(TSource));
            if (TypeInfo.MatchColumns.Count == 0 || TypeInfo.RetrieveColumns.Count == 0)
                throw new ETLBoxException("Please define either a transformation function or use the MatchColumn / RetrieveColumn attributes.");
            TransformationFunc = FindRowByAttributes;
        }

        private TInput FindRowByAttributes(TInput row, ICollection<TSource> cache)
        {
            var lookupHit = cache.FindFirst(e =>
            {
                bool same = true;
                foreach (var mc in TypeInfo.MatchColumns)
                {
                    same &= mc.PropInInput.GetValue(row).Equals(mc.PropInOutput.GetValue(e));
                    if (!same) break;
                }
                return same;
            });
            if (lookupHit != null)
            {
                foreach (var rc in TypeInfo.RetrieveColumns)
                {
                    var retrieveValue = rc.PropInOutput.GetValue(lookupHit);
                    rc.PropInInput.TrySetValue(row, retrieveValue);
                }
            }
            return row;
        }


        //private void LoadLookupData(ICache<TInput, TSourceOutput> cache)
        //{
        //    //NLogStartOnce();
        //    Source.Execute();
        //    LookupBuffer.Wait();
        //    foreach (TSourceOutput rec in LookupBuffer.Data)
        //        cache.Records.Add(rec);
        //}

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

}
