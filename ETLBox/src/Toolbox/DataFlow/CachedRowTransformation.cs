using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Transformations
{
    public class CachedRowTransformation<TInput, TOutput, TCache> : RowTransformation<TInput, TOutput>
        where TCache : class
    {

        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = "Execute cached row transformation";

        public new Func<TInput, ICollection<TCache>, TOutput> TransformationFunc { get; set; }

        /// <summary>
        /// The init action is executed shortly before the first data row is processed.
        /// </summary>
        public new Action<ICacheManager<TInput, TCache>> InitAction { get; set; }

        public ICacheManager<TInput, TCache> CacheManager { get; set; } = new MemoryCache<TInput, TCache>();

        #endregion

        #region Constructors

        public CachedRowTransformation()
        {
        }

        /// <param name="transformationFunc">Will set the <see cref="TransformationFunc"/></param>
        public CachedRowTransformation(Func<TInput, ICollection<TCache>, TOutput> transformationFunc) : this()
        {
            TransformationFunc = transformationFunc;
        }

        #endregion

        #region Implementation

        protected override void InvokeInitActionOnce()
        {
            if (!WasInitActionInvoked)
            {
                CacheManager.Init();
                InitAction?.Invoke(CacheManager);
                WasInitActionInvoked = true;
            }
        }

        protected override TOutput InvokeTransformationFunc(TInput row)
        {
            TOutput result = default;

            if (!CacheManager.Contains(row))
                CacheManager.Add(row);
            result = TransformationFunc.Invoke(row, CacheManager.Records);                
            LogProgress();
            return result;
        }

        #endregion
    }

    /// <inheritdoc />
    public class CachedRowTransformation<TInput> : CachedRowTransformation<TInput, TInput, TInput>
        where TInput : class
    {
        public CachedRowTransformation() : base() { }
        public CachedRowTransformation(Func<TInput, ICollection<TInput>, TInput> rowTransformationFunc) : base(rowTransformationFunc) { }
    }

    /// <inheritdoc />
    public class CachedRowTransformation : CachedRowTransformation<ExpandoObject>
    {
        public CachedRowTransformation() : base() { }
        public CachedRowTransformation(Func<ExpandoObject, ICollection<ExpandoObject>, ExpandoObject> rowTransformationFunc) : base(rowTransformationFunc) { }
    }
}
