using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Define your own destination block.
    /// </summary>
    /// <typeparam name="TInput">Type of datasoure input.</typeparam>
    public class CustomDestination<TInput> : DataFlowTask, ITask, IDataFlowDestination<TInput>
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = $"Write data into custom target";

        /* Public properties */
        public ITargetBlock<TInput> TargetBlock => TargetActionBlock;
        public Action<TInput> WriteAction
        {
            get
            {
                return _writeAction;
            }
            set
            {
                _writeAction = value;
                TargetActionBlock = new ActionBlock<TInput>(AddLogging(_writeAction));
                Completion = AwaitCompletion();
            }
        }
        public Action OnCompletion { get; set; }
        public Task Completion { get; private set; }

        /* Private stuff */
        private Action<TInput> _writeAction;
        internal ActionBlock<TInput> TargetActionBlock { get; set; }

        public CustomDestination()
        {
        }

        public CustomDestination(Action<TInput> writeAction) : this()
        {
            WriteAction = writeAction;
        }

        internal CustomDestination(ITask callingTask, Action<TInput> writeAction) : this(writeAction)
        {
            CopyTaskProperties(callingTask);
        }

        public CustomDestination(string taskName, Action<TInput> writeAction) : this(writeAction)
        {
            this.TaskName = taskName;
        }

        public void Wait()
        {
            Completion.Wait();
        }

        public async Task AwaitCompletion()
        {
            await TargetActionBlock.Completion.ConfigureAwait(false);
            CleanUp();
        }

        private void CleanUp()
        {
            OnCompletion?.Invoke();
            NLogFinish();
        }

        private Action<TInput> AddLogging(Action<TInput> writeAction)
        {
            return new Action<TInput>(
                input =>
                {
                    if (ProgressCount == 0) NLogStart();
                    writeAction.Invoke(input);
                    LogProgress();
                });
        }
    }

    /// <summary>
    /// Define your own destination block. The non generic implementation accepts a string array as input.
    /// </summary>
    public class CustomDestination : CustomDestination<string[]>
    {
        public CustomDestination() : base()
        { }

        public CustomDestination(Action<string[]> writeAction) : base(writeAction)
        { }
    }
}
