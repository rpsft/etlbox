using ETLBox.ControlFlow;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
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
    public class Multicast<TInput> : DataFlowTransformation<TInput, TInput>, ITask, IDataFlowTransformation<TInput, TInput>
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = "Multicast - duplicate data";

        /* Public Properties */
        public override ISourceBlock<TInput> SourceBlock => BroadcastBlock;
        public override ITargetBlock<TInput> TargetBlock => BroadcastBlock;

        /* Private stuff */
        internal BroadcastBlock<TInput> BroadcastBlock { get; set; }
        TypeInfo TypeInfo { get; set; }
        ObjectCopy<TInput> ObjectCopy { get; set; }
        public Multicast()
        {
            TypeInfo = new TypeInfo(typeof(TInput)).GatherTypeInfo();
            ObjectCopy = new ObjectCopy<TInput>(TypeInfo);
            BroadcastBlock = new BroadcastBlock<TInput>(Clone);
        }

        public Multicast(string name) : this()
        {
            this.TaskName = name;
        }

        private TInput Clone(TInput row)
        {
            TInput clone = ObjectCopy.Clone(row);
            LogProgress();
            return clone;
        }

    }

    /// <summary>
    /// A multicast duplicates data from the input into two outputs. The non generic version or the multicast
    /// excepct a dynamic object as input and has two output with the copies of the input.
    /// </summary>
    /// <see cref="Multicast{TInput}"></see>
    /// <example>
    /// <code>
    /// //Non generic Multicast works with dynamic object as input and output
    /// Multicast multicast = new Multicast();
    /// multicast.LinkTo(dest1);
    /// multicast.LinkTo(dest2);
    /// </code>
    /// </example>
    public class Multicast : Multicast<ExpandoObject>
    {
        public Multicast() : base() { }

        public Multicast(string name) : base(name) { }
    }
}
