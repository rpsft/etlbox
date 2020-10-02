using ETLBox.Exceptions;
using System;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// Define your own source block. This block generates data from a your own custom written functions.
    /// </summary>
    /// <example>
    /// <code>
    ///  List&lt;string&gt; Data = new List&lt;string&gt;()
    ///  {
    ///      "Test1", "Test2", "Test3"
    ///  };
    ///  var source = new CustomSource&lt;MyRow&gt;();
    ///  source.ReadFunc = progressCount =&gt;
    ///  {
    ///      return new MyRow()
    ///      {
    ///          Id = progressCount + 1,
    ///          Value = Data[progressCount]
    ///      };    
    ///     return result;
    ///  };
    /// source.ReadCompletedFunc =  progressCount =&gt; progressCount &gt;= Data.Count;
    /// </code>
    /// </example>
    /// <typeparam name="TOutput">Type of outgoing data.</typeparam>
    public class CustomSource<TOutput> : DataFlowExecutableSource<TOutput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName => $"Read data from custom source";

        /// <summary>
        /// The function that returns a data row as output. An integer value with the
        /// current progress count is the input of the function.
        /// </summary>
        public Func<int, TOutput> ReadFunc { get; set; }

        /// <summary>
        /// This predicate returns true when all rows for the flow are successfully returned from the <see cref="ReadFunc"/>. An integer value with the
        /// current progress count is the input of the predicate.
        /// </summary>
        public Predicate<int> ReadingCompleted { get; set; }
        
        #endregion

        #region Constructors

        public CustomSource()
        {
        }

        /// <param name="readFunc">Sets the <see cref="ReadFunc"/></param>
        /// <param name="readingCompleted">Sets the <see cref="ReadingCompleted"/></param>
        public CustomSource(Func<int, TOutput> readFunc, Predicate<int> readingCompleted) : this()
        {
            ReadFunc = readFunc;
            ReadingCompleted = readingCompleted;
        }

        #endregion

        #region Implement abstract methods

        protected override void OnExecutionDoSynchronousWork() { }

        protected override void OnExecutionDoAsyncWork()
        {
            NLogStartOnce();
            ReadAllRecords();
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        #endregion

        #region Implementation

        private void ReadAllRecords()
        {
            while (!ReadingCompleted.Invoke(ProgressCount))
            {
                TOutput result = default;
                try
                {
                    result = ReadFunc.Invoke(ProgressCount);
                    if (!Buffer.SendAsync(result).Result)
                        throw new ETLBoxException("Buffer already completed or faulted!", this.Exception);
                }
                catch (ETLBoxException) { throw; }
                catch (Exception e)
                {
                    ThrowOrRedirectError(e, e.Message);
                }

                LogProgress();
            }
        }

        #endregion

    }

    /// <inheritdoc/>
    public class CustomSource : CustomSource<ExpandoObject>
    {
        public CustomSource() : base()
        { }

        public CustomSource(Func<int, ExpandoObject> readFunc, Predicate<int> readingCompleted) : base(readFunc, readingCompleted)
        { }
    }
}
