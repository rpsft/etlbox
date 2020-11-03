//using System;
//using System.Collections.Generic;
//using System.Dynamic;
//using System.Threading.Tasks.Dataflow;

//namespace ETLBox.DataFlow.Transformations
//{
//    public class CachedRowTransformation<TInput, TOutput, TCache> : DataFlowTransformation<TInput, TOutput>
//        where TCache : class
//    {

//        #region Public properties

//        /// <inheritdoc/>
//        public override string TaskName { get; set; } = "Execute cached row transformation";

//        public Func<TInput, ICollection<TCache>, TOutput> TransformationFunc { get; set; }

//        //public Func<TInput, Cache<TCache>,bool> CacheContainsRowFunc { get; set; }

//        //public Action<TInput,Cache<TCache>> FillCacheAction { get; set; }

        
//        /// <summary>
//        /// The init action is executed shortly before the first data row is processed.
//        /// </summary>
//        public Action<ICache<TInput, TCache>> InitAction { get; set; }

//        /// <inheritdoc />
//        public override ITargetBlock<TInput> TargetBlock => TransformBlock;

//        /// <inheritdoc />
//        public override ISourceBlock<TOutput> SourceBlock => TransformBlock;

//        #endregion

//        #region Constructors

//        public CachedRowTransformation()
//        {
//        }

//        /// <param name="transformationFunc">Will set the <see cref="TransformationFunc"/></param>
//        public CachedRowTransformation(Func<TInput, ICollection<TCache>, TOutput> transformationFunc) : this()
//        {
//            TransformationFunc = transformationFunc;
//        }

//        #endregion

//        #region Implement abstract methods

//        protected override void InternalInitBufferObjects()
//        {
//            TransformBlock = new TransformBlock<TInput, TOutput>(
//                row =>
//                {
//                    NLogStartOnce();
//                    try
//                    {
//                        InvokeInitActionOnce();
//                        return InvokeTransformationFunc(row);
//                    }
//                    catch (Exception e)
//                    {
//                        ThrowOrRedirectError(e, ErrorSource.ConvertErrorData<TInput>(row));
//                        return default;
//                    }
//                }, new ExecutionDataflowBlockOptions()
//                {
//                    BoundedCapacity = MaxBufferSize,
//                }
//            );
//        }

//        protected override void CleanUpOnSuccess()
//        {
//            NLogFinishOnce();
//        }

//        protected override void CleanUpOnFaulted(Exception e) { }

//        #endregion

//        #region Implementation

//        TransformBlock<TInput, TOutput> TransformBlock;
//        bool WasInitActionInvoked;
//        public ICache<TInput, TCache> CacheManager = new PartialTableCache<TInput, TCache>();

//        private void InvokeInitActionOnce()
//        {
//            if (!WasInitActionInvoked)
//            {
//                //CacheManager = 
//                InitAction?.Invoke(CacheManager);
//                WasInitActionInvoked = true;
//            }
//        }

//        private TOutput InvokeTransformationFunc(TInput row)
//        {
//            TOutput result = default;

//            if (!CacheManager.Contains(row))
//                CacheManager.Add(row);
//            result = TransformationFunc.Invoke(row, CacheManager.Records);                
//            LogProgress();
//            return result;
//        }

//        #endregion

      
       
//    }

//    /// <inheritdoc />
//    //public class RowTransformation<TInput> : RowTransformation<TInput, TInput>
//    //{
//    //    public RowTransformation() : base() { }
//    //    public RowTransformation(Func<TInput, TInput> rowTransformationFunc) : base(rowTransformationFunc) { }
//    //    public RowTransformation(Func<TInput, TInput> rowTransformationFunc, Action initAction) : base(rowTransformationFunc, initAction) { }
//    //}

//    /// <inheritdoc />
//    //public class RowTransformation : RowTransformation<ExpandoObject>
//    //{
//    //    public RowTransformation() : base() { }
//    //    public RowTransformation(Func<ExpandoObject, ExpandoObject> rowTransformationFunc) : base(rowTransformationFunc) { }
//    //    public RowTransformation(Func<ExpandoObject, ExpandoObject> rowTransformationFunc, Action initAction) : base(rowTransformationFunc, initAction) { }
//    //}
//}
