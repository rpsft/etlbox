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
    /// <typeparam name="TSourceOutput">Type of data used in the lookup source.</typeparam>
    public class LookupTransformation<TInput, TSourceOutput> : DataFlowTransformation<TInput, TInput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = "Lookup";

        /// <summary>
        /// Holds the data read from the lookup source. This data is used to find data that is missing in the incoming rows.
        /// </summary>
        public IList<TSourceOutput> LookupData
        {
            get
            {
                return LookupBuffer.Data; //as List<TSourceOutput>;
            }
            set
            {
                LookupBuffer.Data = value;
            }
        }

        /// <inheritdoc/>
        public override ISourceBlock<TInput> SourceBlock => RowTransformation.SourceBlock;

        /// <inheritdoc/>
        public override ITargetBlock<TInput> TargetBlock => RowTransformation.TargetBlock;
        /// <summary>
        /// The source component from which the lookup data is retrieved. E.g. a <see cref="DbSource"/> or a <see cref="MemorySource"/>.
        /// </summary>
        public IDataFlowExecutableSource<TSourceOutput> Source { get; set; }

        /// <summary>
        /// A transformation func that describes how the ingoing data can be enriched with the already pre-read data from
        /// the <see cref="Source"/>
        /// </summary>
        public Func<TInput, TInput> TransformationFunc { get; set; }

        #endregion

        #region Constructors

        public LookupTransformation()
        {
            LookupBuffer = new MemoryDestination<TSourceOutput>();
            RowTransformation = new RowTransformation<TInput, TInput>();
        }

        /// <param name="lookupSource">Sets the <see cref="Source"/> of the lookup.</param>
        public LookupTransformation(IDataFlowExecutableSource<TSourceOutput> lookupSource) : this()
        {
            Source = lookupSource;
        }

        /// <param name="lookupSource">Sets the <see cref="Source"/> of the lookup.</param>
        /// <param name="transformationFunc">Sets the <see cref="TransformationFunc"/></param>
        public LookupTransformation(IDataFlowExecutableSource<TSourceOutput> lookupSource, Func<TInput, TInput> transformationFunc)
            : this(lookupSource)
        {
            TransformationFunc = transformationFunc;
        }

        /// <param name="lookupSource">Sets the <see cref="Source"/> of the lookup.</param>
        /// <param name="transformationFunc">Sets the <see cref="TransformationFunc"/></param>
        /// <param name="lookupList">Sets the list for the <see cref="LookupData"/></param>
        public LookupTransformation(IDataFlowExecutableSource<TSourceOutput> lookupSource, Func<TInput, TInput> transformationFunc, IList<TSourceOutput> lookupList)
            : this(lookupSource, transformationFunc)
        {
            LookupData = lookupList;
        }

        #endregion

        #region Implement abstract methods

        protected override void InternalInitBufferObjects()
        {
            if (TransformationFunc == null)
                DefaultFuncWithMatchRetrieveAttributes();
            InitRowTransformationManually(LoadLookupData);

            LinkInternalLoadBufferFlow();
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        internal override void CompleteBufferOnPredecessorCompletion() => RowTransformation.CompleteBufferOnPredecessorCompletion();

        internal override void FaultBufferOnPredecessorCompletion(Exception e) => RowTransformation.FaultBufferOnPredecessorCompletion(e);

        public new IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
        {
            var errorSource = InternalLinkErrorTo(target);
            RowTransformation.ErrorSource = new ErrorSource() { Redirection = this.ErrorSource };
            Source.ErrorSource = new ErrorSource() { Redirection = this.ErrorSource };
            return errorSource;
        }

        #endregion

        #region Implementation

        MemoryDestination<TSourceOutput> LookupBuffer;
        RowTransformation<TInput, TInput> RowTransformation;
        LookupTypeInfo TypeInfo;

        private void InitRowTransformationManually(Action initAction)
        {
            RowTransformation.TransformationFunc = TransformationFunc;
            RowTransformation.CopyLogTaskProperties(this);
            RowTransformation.InitAction = initAction;
            RowTransformation.MaxBufferSize = this.MaxBufferSize;
            RowTransformation.InitBufferObjects();
        }

        private void LinkInternalLoadBufferFlow()
        {
            if (Source == null) throw new ETLBoxException("You need to define a lookup source before using a LookupTransformation in a data flow");
            LookupBuffer.CopyLogTaskProperties(this);
            Source.LinkTo(LookupBuffer);
        }

        private void DefaultFuncWithMatchRetrieveAttributes()
        {
            TypeInfo = new LookupTypeInfo(typeof(TInput), typeof(TSourceOutput));
            if (TypeInfo.MatchColumns.Count == 0 || TypeInfo.RetrieveColumns.Count == 0)
                throw new ETLBoxException("Please define either a transformation function or use the MatchColumn / RetrieveColumn attributes.");
            TransformationFunc = FindRowByAttributes;
        }

        private TInput FindRowByAttributes(TInput row)
        {
            var lookupHit = FindFirst(LookupData, e =>
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

        public static T FindFirst<T>(IList<T> source, Func<T, bool> condition)
        {
            foreach (T item in source)
                if (condition(item))
                    return item;
            return default(T);
        }

        private void LoadLookupData()
        {
            NLogStartOnce();
            Source.Execute();
            LookupBuffer.Wait();
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

        public LookupTransformation(IDataFlowExecutableSource<ExpandoObject> lookupSource, Func<ExpandoObject, ExpandoObject> transformationFunc)
            : base(lookupSource, transformationFunc)
        { }

        public LookupTransformation(IDataFlowExecutableSource<ExpandoObject> lookupSource, Func<ExpandoObject, ExpandoObject> transformationFunc, List<ExpandoObject> lookupList)
            : base(lookupSource, transformationFunc, lookupList)
        { }
    }

}
