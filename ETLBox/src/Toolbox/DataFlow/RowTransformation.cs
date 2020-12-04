using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// The RowTransformation will apply the transformation function to each row of data.
    /// </summary>
    /// <typeparam name="TInput">The type of ingoing data.</typeparam>
    /// <typeparam name="TOutput">The type of outgoing data.</typeparam>
    /// <see cref="RowTransformation"/>
    /// <example>
    /// <code>
    /// RowTransformation&lt;InputType, OutputType&gt; trans = new RowTransformation&lt;InputType, OutputType&gt;(
    /// row => {
    ///     return new OutputType() { Value = row.Value + 1 };
    /// });
    /// </code>
    /// </example>
    public class RowTransformation<TInput, TOutput> : DataFlowTransformation<TInput, TOutput>
    {

        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = "Execute row transformation";

        /// <summary>
        /// Each ingoing row will be transformed using this function.
        /// </summary>
        public Func<TInput, TOutput> TransformationFunc { get; set; }

        /// <summary>
        /// The init action is executed shortly before the first data row is processed.
        /// </summary>
        public Action InitAction { get; set; }

        /// <inheritdoc />
        public override ITargetBlock<TInput> TargetBlock => TransformBlock;

        /// <inheritdoc />
        public override ISourceBlock<TOutput> SourceBlock => TransformBlock;

        #endregion

        #region Constructors

        public RowTransformation()
        {
        }

        /// <param name="transformationFunc">Will set the <see cref="TransformationFunc"/></param>
        public RowTransformation(Func<TInput, TOutput> transformationFunc) : this()
        {
            TransformationFunc = transformationFunc;
        }

        /// <param name="transformationFunc">Will set the <see cref="TransformationFunc"/></param>
        /// <param name="initAction">Will set the <see cref="InitAction"/></param>
        public RowTransformation(Func<TInput, TOutput> transformationFunc, Action initAction) : this(transformationFunc)
        {
            this.InitAction = initAction;
        }

        #endregion

        #region Implement abstract methods

        protected override void CheckParameter() { }

        protected override void InitComponent()
        {
            TransformBlock = new TransformBlock<TInput, TOutput>(
                WrapTransformationFunc, 
                new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = MaxBufferSize,
                    CancellationToken = CancellationSource.Token
                }
            );
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        #endregion

        #region Implementation

        TransformBlock<TInput, TOutput> TransformBlock;
        protected bool WasInitActionInvoked;

        private TOutput WrapTransformationFunc(TInput row)
        {
            NLogStartOnce();
            try
            {
                InvokeInitActionOnce();
                return InvokeTransformationFunc(row);
            }
            catch (System.Threading.Tasks.TaskCanceledException te) {
                throw te;
            }
            catch (Exception e)
            {
                ThrowOrRedirectError(e, ErrorSource.ConvertErrorData<TInput>(row));
                return default;
            }
        }

        protected virtual void InvokeInitActionOnce()
        {
            if (!WasInitActionInvoked)
            {
                InitAction?.Invoke();
                WasInitActionInvoked = true;
            }
        }

        protected virtual TOutput InvokeTransformationFunc(TInput row)
        {
            TOutput result = default;
            result = TransformationFunc.Invoke(row);
            LogProgress();
            return result;
        }

        #endregion
    }

    /// <inheritdoc />
    public class RowTransformation<TInput> : RowTransformation<TInput, TInput>
    {
        public RowTransformation() : base() { }
        public RowTransformation(Func<TInput, TInput> rowTransformationFunc) : base(rowTransformationFunc) { }
        public RowTransformation(Func<TInput, TInput> rowTransformationFunc, Action initAction) : base(rowTransformationFunc, initAction) { }
    }

    /// <inheritdoc />
    public class RowTransformation : RowTransformation<ExpandoObject>
    {
        public RowTransformation() : base() { }
        public RowTransformation(Func<ExpandoObject, ExpandoObject> rowTransformationFunc) : base(rowTransformationFunc) { }
        public RowTransformation(Func<ExpandoObject, ExpandoObject> rowTransformationFunc, Action initAction) : base(rowTransformationFunc, initAction) { }
    }
}
