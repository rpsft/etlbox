using System;
using System.Reflection;
using System.Threading.Tasks.Dataflow;


namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// A multicast duplicates data from the input into two outputs.
    /// </summary>
    /// <typeparam name="TInput">Type of input data.</typeparam>
    /// <example>
    /// <code>
    /// Multicast&lt;MyDataRow&gt; multicast = new Multicast&lt;MyDataRow&gt;();
    /// multicast.LinkTo(dest1);
    /// multicast.LinkTo(dest2);
    /// </code>
    /// </example>
    public class Multicast<TInput> : GenericTask, ITask, IDataFlowTransformation<TInput, TInput>
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DF_MULTICAST";
        public override string TaskName { get; set; } = "Multicast (unnamed)";
        public override void Execute() { throw new Exception("Transformations can't be executed directly"); }

        /* Public Properties */
        public ISourceBlock<TInput> SourceBlock => BroadcastBlock;
        public ITargetBlock<TInput> TargetBlock => BroadcastBlock;

        /* Private stuff */
        internal BroadcastBlock<TInput> BroadcastBlock { get; set; }
        NLog.Logger NLogger { get; set; }
        TypeInfo TypeInfo { get; set; }
        public Multicast()
        {
            NLogger = NLog.LogManager.GetLogger("ETL");
            TypeInfo = new TypeInfo(typeof(TInput));
            BroadcastBlock = new BroadcastBlock<TInput>(Clone);
        }

        public Multicast(string name) : this()
        {
            this.TaskName = name;
        }

        public void LinkTo(IDataFlowLinkTarget<TInput> target)
        {
            BroadcastBlock.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        public void LinkTo(IDataFlowLinkTarget<TInput> target, Predicate<TInput> predicate)
        {
            BroadcastBlock.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true }, predicate);
            NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        private TInput Clone(TInput row)
        {
            TInput clone = default(TInput);
            if (TypeInfo.IsArray)
            {
                Array source = row as Array;
                clone = (TInput)Activator.CreateInstance(typeof(TInput), new object[] { source.Length });
                Array dest = clone as Array;
                Array.Copy(source, dest, source.Length);
            }
            else
            {
                clone = (TInput)Activator.CreateInstance(typeof(TInput));
                foreach (PropertyInfo propInfo in TypeInfo.Properties)
                {
                    propInfo.SetValue(clone, propInfo.GetValue(row));
                }
            }
            return clone;
        }
    }

    /// <summary>
    /// A multicast duplicates data from the input into two outputs. The non generic version or the multicast
    /// excepct a string array as input and has two output with the copies of the incoming stríng array.
    /// </summary>
    /// <see cref="Multicast{TInput}"></see>
    /// <example>
    /// <code>
    /// //Non generic Multicast works with string[] as input and output
    /// Multicast multicast = new Multicast();
    /// multicast.LinkTo(dest1);
    /// multicast.LinkTo(dest2);
    /// </code>
    /// </example>
    public class Multicast : Multicast<string[]>
    {
        public Multicast() : base() { }

        public Multicast(string name) : base(name) { }
    }
}
