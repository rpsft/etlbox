using System;
using System.Collections.Generic;
using System.Threading.Tasks.Dataflow;


namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// A block transformation will wait for all data to be loaded into the buffer before the transformation is applied. After all data is in the buffer, the transformation
    /// is execution and the result posted into the targets.
    /// </summary>
    /// <typeparam name="TInput">Type of data input</typeparam>
    /// <typeparam name="TOutput">Type of data output</typeparam>
    /// <example>
    /// <code>
    /// BlockTransformation&lt;MyInputRow, MyOutputRow&gt; block = new BlockTransformation&lt;MyInputRow, MyOutputRow&gt;(
    ///     inputDataAsList => {
    ///         return inputData.Select( inputRow => new MyOutputRow() { Value2 = inputRow.Value1 }).ToList();
    ///     });
    /// block.LinkTo(dest);
    /// </code>
    /// </example>
    public class BlockTransformation<TInput, TOutput> : DataFlowTransformation<TInput, TOutput>, ITask, IDataFlowTransformation<TInput, TOutput>
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = "Block Transformation";

        /* Public Properties */
        public Func<List<TInput>, List<TOutput>> BlockTransformationFunc
        {
            get
            {
                return _blockTransformationFunc;
            }
            set
            {
                _blockTransformationFunc = value;
                InputBuffer = new ActionBlock<TInput>(row => InputData.Add(row));
                InputBuffer.Completion.ContinueWith(t =>
                {
                    OutputData = BlockTransformationFunc(InputData);
                    WriteIntoOutput();
                });

            }
        }
        public override ISourceBlock<TOutput> SourceBlock => OutputBuffer;
        public override ITargetBlock<TInput> TargetBlock => InputBuffer;

        /* Private stuff */
        BufferBlock<TOutput> OutputBuffer { get; set; }
        ActionBlock<TInput> InputBuffer { get; set; }
        Func<List<TInput>, List<TOutput>> _blockTransformationFunc;
        List<TInput> InputData { get; set; }
        List<TOutput> OutputData { get; set; }
        public BlockTransformation()
        {
            InputData = new List<TInput>();
            OutputBuffer = new BufferBlock<TOutput>();
        }

        public BlockTransformation(Func<List<TInput>, List<TOutput>> blockTransformationFunc) : this()
        {
            BlockTransformationFunc = blockTransformationFunc;
        }

        public BlockTransformation(string name, Func<List<TInput>, List<TOutput>> blockTransformationFunc) : this(blockTransformationFunc)
        {
            this.TaskName = name;
        }

        public BlockTransformation(ITask task, Func<List<TInput>, List<TOutput>> blockTransformationFunc) : this(blockTransformationFunc)
        {
            CopyTaskProperties(task);
        }

        private void WriteIntoOutput()
        {
            NLogStart();
            foreach (TOutput row in OutputData)
            {
                OutputBuffer.SendAsync(row).Wait();
                LogProgress();
            }
            OutputBuffer.Complete();
            NLogFinish();
        }
    }

    /// <summary>
    /// A block transformation will wait for all data to be loaded into the buffer before the transformation is applied. After all data is in the buffer, the transformation
    /// is execution and the result posted into the targets.
    /// </summary>
    /// <typeparam name="TInput">Type of data input (equal type of data output)</typeparam>
    /// <example>
    /// <code>
    /// BlockTransformation&lt;MyDataRow&gt; block = new BlockTransformation&lt;MyDataRow&gt;(
    ///     inputData => {
    ///         return inputData.Select( row => new MyDataRow() { Value1 = row.Value1, Value2 = 3 }).ToList();
    ///     });
    /// block.LinkTo(dest);
    /// </code>
    /// </example>
    public class BlockTransformation<TInput> : BlockTransformation<TInput, TInput>
    {
        public BlockTransformation(Func<List<TInput>, List<TInput>> blockTransformationFunc) : base(blockTransformationFunc)
        { }

        public BlockTransformation(string name, Func<List<TInput>, List<TInput>> blockTransformationFunc) : base(name, blockTransformationFunc)
        { }

        public BlockTransformation(ITask task, Func<List<TInput>, List<TInput>> blockTransformationFunc) : base(task, blockTransformationFunc)
        { }
    }

    /// <summary>
    /// A block transformation will wait for all data to be loaded into the buffer before the transformation is applied. After all data is in the buffer, the transformation
    /// is execution and the result posted into the targets.
    /// The non generic implementation uses a string array as input and output
    /// </summary>
    public class BlockTransformation : BlockTransformation<string[]>
    {
        public BlockTransformation(Func<List<string[]>, List<string[]>> blockTransformationFunc) : base(blockTransformationFunc)
        { }

        public BlockTransformation(string name, Func<List<string[]>, List<string[]>> blockTransformationFunc) : base(name, blockTransformationFunc)
        { }

        public BlockTransformation(ITask task, Func<List<string[]>, List<string[]>> blockTransformationFunc) : base(task, blockTransformationFunc)
        { }
    }

}
