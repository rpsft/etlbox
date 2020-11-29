using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// A block transformation will wait for all data from the flow to be loaded into its buffer.
    /// After all data is in the buffer, the transformation function
    /// is executed for the complete data and the result posted into the targets.
    /// The block transformations allows you to access all data in the flow in one generic collection.
    /// But as this block any processing until all data is buffered, it will also need to store the whole data in memory.
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data.</typeparam>
    /// <typeparam name="TOutput">Type of outgoing data.</typeparam>
    /// <example>
    /// <code>
    /// BlockTransformation&lt;InputType&gt; block = new BlockTransformation&lt;InputType&gt;(
    ///     inputData => {
    ///         inputData.RemoveRange(1, 2);
    ///         inputData.Add(new InputType() { Value = 1 });
    ///         return inputData;
    /// });
    /// </code>
    /// </example>
    public class BlockTransformation<TInput, TOutput> : BatchTransformation<TInput, TOutput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = "Excecute block transformation";

        /// <inheritdoc/>
        public override int BatchSize
        {
            get
            {
                return int.MaxValue;
            }
            set
            {
                throw new ETLBoxException("The BlockTransformation will load all data into memory - use the BatchTransformation " +
                    "if you need to access smaller portions of your data.");
            }
        }

        /// <inheritdoc/>
        public override int MaxBufferSize 
        {
            get
            {
                return -1;
            }
            set
            {
                throw new ETLBoxException("The BlockTransformation will load all data into memory - use the BatchTransformation " +
                    "if you need to access smaller portions of your data.");
            }
        }

        /// <summary>
        /// The transformation Func that is executed on the complete input data. It needs
        /// to return an array of output data, which doesn't need have to be the same length
        /// as the input array. 
        /// </summary>
        public Func<TInput[], TOutput[]> BlockTransformationFunc
        {
            get
            {
                return this.BatchTransformationFunc;
            }
            set
            {
                this.BatchTransformationFunc = value;
            }
        }

        #endregion

        #region Constructors

        public BlockTransformation() : base()
        {
        }

        /// <param name="blockTransformationFunc">Sets the <see cref="BlockTransformationFunc"/></param>
        public BlockTransformation(Func<TInput[], TOutput[]> blockTransformationFunc) : this()
        {
            BlockTransformationFunc = blockTransformationFunc;
        }

        #endregion
    }

    /// <inheritdoc/>
    public class BlockTransformation<TInput> : BlockTransformation<TInput, TInput>
    {
        public BlockTransformation() : base()
        { }
        public BlockTransformation(Func<TInput[], TInput[]> blockTransformationFunc) : base(blockTransformationFunc)
        { }

    }

    /// <inheritdoc/>
    public class BlockTransformation : BlockTransformation<ExpandoObject>
    {
        public BlockTransformation() : base()
        { }

        public BlockTransformation(Func<ExpandoObject[], ExpandoObject[]> blockTransformationFunc) : base(blockTransformationFunc)
        { }

    }
}
