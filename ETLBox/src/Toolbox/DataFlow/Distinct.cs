using ETLBox.DataFlow.Connectors;
using ETLBox.Exceptions;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    public class Distinct<TInput> : DataFlowTransformation<TInput, TInput>   
        where TInput : class
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = "Distinct";

        /// <inheritdoc/>
        public override ISourceBlock<TInput> SourceBlock => CachedRowTransformation.SourceBlock;

        /// <inheritdoc/>
        public override ITargetBlock<TInput> TargetBlock => CachedRowTransformation.TargetBlock;

        /// <summary>
        /// Contains the property names that are used to determine if 
        /// two objects are equal
        /// </summary>
        public new int ProgressCount => CachedRowTransformation.ProgressCount;
        
        public ICollection<DistinctColumn> DistinctColumns { get; set; }

        #endregion

        #region Constructors

        public Distinct()
        {
            CachedRowTransformation = new CachedRowTransformation<TInput, TInput, HashValue>();
            TypeInfo = new DistinctTypeInfo(typeof(TInput));
        }
       
        #endregion

        #region Implement abstract methods

        protected override void InternalInitBufferObjects()
        {

            InitRowTransformationManually();
        }


        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        internal override void CompleteBuffer() => CachedRowTransformation.CompleteBuffer();

        internal override void FaultBuffer(Exception e) => CachedRowTransformation.FaultBuffer(e);

        public new IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
        {
            var errorSource = InternalLinkErrorTo(target);
            CachedRowTransformation.ErrorSource = new ErrorSource() { Redirection = this.ErrorSource };
            return errorSource;
        }

        #endregion

        #region Implementation

        CachedRowTransformation<TInput, TInput, HashValue> CachedRowTransformation;
        DistinctTypeInfo TypeInfo;

        private void InitRowTransformationManually()
        {
            var hashCache = new HashCache<TInput>();
            hashCache.HashSumFunc = HashSumCalculation;
            CachedRowTransformation.CacheManager = hashCache;
            CachedRowTransformation.TransformationFunc = MakeRowDistinct;
            CachedRowTransformation.CopyLogTaskProperties(this);
            CachedRowTransformation.MaxBufferSize = this.MaxBufferSize;
            CachedRowTransformation.FillCacheAfterTranformation = true;
            CachedRowTransformation.CancellationSource = this.CancellationSource;
            CachedRowTransformation.InitBufferObjects();
        }

        private TInput MakeRowDistinct(TInput row, ICollection<HashValue> cache)
        {
            var cm = CachedRowTransformation.CacheManager as HashCache<TInput>;
            if (cm.Contains(row))
                return null;
            else
                return row;
        }

        private int HashSumCalculation(TInput row)
        {
            List<object> values = new List<object>();
            foreach (var prop in GetDistinctColumns())
                values.Add(prop.GetValue(row));
            return HashHelper.HashSum(values);
        }

        private PropertyInfo[] GetDistinctColumns()
        {
            if (TypeInfo.DistinctColumns.Count > 0)
                return TypeInfo.PropertiesByName
                    .Where(d => TypeInfo.DistinctColumns.Contains(d.Key))
                    .Select(d => d.Value)
                    .ToArray();
            else if (DistinctColumns?.Count > 0 && DistinctColumns.All(dc => !string.IsNullOrEmpty(dc.DistinctPropertyName)))
                return TypeInfo.PropertiesByName
                    .Where(d => DistinctColumns.Select(c => c.DistinctPropertyName).Contains(d.Key))
                    .Select(d => d.Value)
                    .ToArray();
            else
                return TypeInfo.Properties;
        }

        #endregion

    }


}
