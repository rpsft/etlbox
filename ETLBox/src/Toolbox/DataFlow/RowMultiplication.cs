using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks.Dataflow;


namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// This transformation allow you to transform your input data into multple output data records.  
    /// </summary>
    /// <typeparam name="TInput">Type of data input</typeparam>
    /// <typeparam name="TOutput">Type of data output</typeparam>
    public class RowMultiplication<TInput, TOutput> : DataFlowTransformation<TInput, TOutput>, ITask, IDataFlowTransformation<TInput, TOutput>
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = $"Duplicate rows.";

        /* Public Properties */
        public override ISourceBlock<TOutput> SourceBlock => OutputBuffer;
        public override ITargetBlock<TInput> TargetBlock => InputBuffer;
        public Func<TInput, IEnumerable<TOutput>> MultiplicationFunc { get; set; }

        /* Private stuff */
        BufferBlock<TOutput> OutputBuffer { get; set; }
        ActionBlock<TInput> InputBuffer { get; set; }
        bool WasInitialized { get; set; }
        internal ErrorHandler ErrorHandler { get; set; } = new ErrorHandler();

        //TypeInfo TypeInfo { get; set; }

        public RowMultiplication()
        {
            //TypeInfo = new TypeInfo(typeof(TInput));
            OutputBuffer = new BufferBlock<TOutput>();
            InputBuffer = new ActionBlock<TInput>(row => MultiplicateRow(row));
            InputBuffer.Completion.ContinueWith(t => FinishInput());
        }

        public RowMultiplication(Func<TInput, IEnumerable<TOutput>> multiplicationFunc) : this()
        {
            MultiplicationFunc = multiplicationFunc;
        }


        public void InitFlow()
        {
            if (!WasInitialized)
            {
                NLogStart();
                WasInitialized = true;
            }
        }
        private void FinishInput()
        {
            OutputBuffer.Complete();
            NLogFinish();
        }

        private void MultiplicateRow(TInput row)
        {
            if (row == null) return;
            try
            {
                IEnumerable<TOutput> multipleOutputs = MultiplicationFunc.Invoke(row);
                foreach (TOutput output in multipleOutputs)
                    OutputBuffer.SendAsync(output).Wait();
            }
            catch (Exception e)
            {
                if (!ErrorHandler.HasErrorBuffer) throw e;
                ErrorHandler.Send(e, ErrorHandler.ConvertErrorData<TInput>(row));
            }
        }

        public void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target)
            => ErrorHandler.LinkErrorTo(target, OutputBuffer.Completion);

    }

    /// <summary>
    /// This transformation allow you to transform your input data into multple output data records.  
    /// </summary>
    /// <see cref="RowMultiplication{TInput, TOutput}"/>
    public class RowMultiplication : RowMultiplication<ExpandoObject,ExpandoObject>
    {
        public RowMultiplication() : base()
        { }

        public RowMultiplication(Func<ExpandoObject, IEnumerable<ExpandoObject>> multiplicationFunc)
            : base (multiplicationFunc) 
        { }
    }

    /// <inheritdoc/>
    public class RowMultiplication<TInput> : RowMultiplication<TInput, TInput>
    {
        public RowMultiplication() : base()
        { }

        public RowMultiplication(Func<TInput, IEnumerable<TInput>> multiplicationFunc)
            : base(multiplicationFunc)
        { }
    }
}
