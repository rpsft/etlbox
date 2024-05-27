using System.Threading;
using ALE.ETLBox.Common.DataFlow;
using ETLBox.Primitives;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Reads data from a memory source. While reading the data from the list, data is also asnychronously posted into the targets.
    /// Data is read a as string from the source and dynamically converted into the corresponding data format.
    /// </summary>
    [PublicAPI]
    public class MemorySource<TOutput> : DataFlowSource<TOutput>, IDataFlowSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskName => "Read data from memory";

        /* Public properties */
        public IEnumerable<TOutput> Data { get; set; }
        public IList<TOutput> DataAsList
        {
            get { return Data as IList<TOutput>; }
            set { Data = value; }
        }

        /* Private stuff */

        public MemorySource()
        {
            Data = new List<TOutput>();
        }

        public MemorySource(IEnumerable<TOutput> data)
        {
            Data = data;
        }

        public override void Execute(CancellationToken cancellationToken)
        {
            LogStart();
            ReadRecordAndSendIntoBuffer();
            LogProgress();
            Buffer.Complete();
            LogFinish();
        }

        private void ReadRecordAndSendIntoBuffer()
        {
            foreach (TOutput record in Data)
            {
                Buffer.SendAsync(record).Wait();
            }
        }
    }

    /// <summary>
    /// Reads data from a memory source. While reading the data from the file, data is also asnychronously posted into the targets.
    /// MemorySource as a nongeneric type always return a dynamic object as output. If you need typed output, use
    /// the MemorySource&lt;TOutput&gt; object instead.
    /// </summary>
    /// <see cref="MemorySource{TOutput}"/>
    [PublicAPI]
    public sealed class MemorySource : MemorySource<ExpandoObject>
    {
        public MemorySource() { }

        public MemorySource(IList<ExpandoObject> data)
            : base(data) { }
    }
}
