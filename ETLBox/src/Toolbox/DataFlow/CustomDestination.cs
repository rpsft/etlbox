namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Define your own destination block.
    /// </summary>
    /// <typeparam name="TInput">Type of datasource input.</typeparam>
    [PublicAPI]
    public class CustomDestination<TInput> : DataFlowDestination<TInput>
    {
        /* ITask Interface */
        public sealed override string TaskName { get; set; } = "Write data into custom target";

        /* Public properties */
        public Action<TInput> WriteAction
        {
            get { return _writeAction; }
            set
            {
                _writeAction = value;
                InitObjects();
            }
        }

        /* Private stuff */
        private Action<TInput> _writeAction;

        public CustomDestination() { }

        public CustomDestination(Action<TInput> writeAction)
            : this()
        {
            WriteAction = writeAction;
        }

        internal CustomDestination(ITask callingTask, Action<TInput> writeAction)
            : this(writeAction)
        {
            CopyTaskProperties(callingTask);
        }

        public CustomDestination(string taskName, Action<TInput> writeAction)
            : this(writeAction)
        {
            TaskName = taskName;
        }

        private void InitObjects()
        {
            TargetAction = new ActionBlock<TInput>(AddLoggingAndErrorHandling(_writeAction));
            SetCompletionTask();
        }

        private Action<TInput> AddLoggingAndErrorHandling(Action<TInput> writeAction)
        {
            return input =>
            {
                if (ProgressCount == 0)
                    NLogStart();
                try
                {
                    if (input != null)
                        writeAction.Invoke(input);
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer)
                        throw;
                    ErrorHandler.Send(e, ErrorHandler.ConvertErrorData(input));
                }
                LogProgress();
            };
        }
    }

    /// <summary>
    /// Define your own destination block. The non generic implementation uses a dynamic object as input.
    /// </summary>
    [PublicAPI]
    public class CustomDestination : CustomDestination<ExpandoObject>
    {
        public CustomDestination() { }

        public CustomDestination(Action<ExpandoObject> writeAction)
            : base(writeAction) { }
    }
}
