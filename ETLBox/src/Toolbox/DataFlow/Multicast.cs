using System;
using System.Reflection;
using System.Threading.Tasks.Dataflow;


namespace ALE.ETLBox.DataFlow {
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
    public class Multicast<TInput> : GenericTask, ITask, IDataFlowTransformation<TInput, TInput> where TInput : new() {
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
        public Multicast() {
            NLogger = NLog.LogManager.GetLogger("ETL");
            TypeInfo = new TypeInfo(typeof(TInput));
            BroadcastBlock = new BroadcastBlock<TInput>(Clone);
        }

        public Multicast(string name) : this() {
            this.TaskName = name;
        }

        public void LinkTo(IDataFlowLinkTarget<TInput> target) {
            BroadcastBlock.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        public void LinkTo(IDataFlowLinkTarget<TInput> target, Predicate<TInput> predicate) {
            BroadcastBlock.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true }, predicate);
            NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        private TInput Clone(TInput row) {
            TInput clone = default(TInput);
            if (!TypeInfo.IsArray) {
                clone = new TInput();                
                foreach (PropertyInfo propInfo in TypeInfo.Properties) {
                    propInfo.SetValue(clone, propInfo.GetValue(row));                    
                }
            }
            return clone;
        }
    }
}
