using System;
using System.Collections.Generic;
using System.Dynamic;
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
        public Multicast()
        {
            TypeInfo = new TypeInfo(typeof(TInput));
            BroadcastBlock = new BroadcastBlock<TInput>(Clone);
        }

        public Multicast(string name) : this()
        {
            this.TaskName = name;
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
            else if(TypeInfo.IsDynamic) {
                    clone = (TInput)Activator.CreateInstance(typeof(TInput));//new ExpandoObject();

                    var _original = (IDictionary<string, object>)row;
                    var _clone = (IDictionary<string, object>)clone;

                    foreach (var kvp in _original)
                        _clone.Add(kvp);
            }
            else
            {
                clone = (TInput)Activator.CreateInstance(typeof(TInput));
                foreach (PropertyInfo propInfo in TypeInfo.Properties)
                {
                    propInfo.SetValue(clone, propInfo.GetValue(row));
                }
            }
            LogProgress();
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
