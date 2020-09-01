using Newtonsoft.Json;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// Works as a source component for any errors. Another component
    /// can use this source to redirect errors into the error data flow.
    /// </summary>
    public class ErrorSource : DataFlowExecutableSource<ETLBoxError>
    {
        /// <summary>
        /// If set to another error source, all message send to this source will redirected.
        /// </summary>
        public ErrorSource Redirection { get; set; }

        public ErrorSource()
        {
        }

        protected override void InternalInitBufferObjects()
        {
            Buffer = new BufferBlock<ETLBoxError>();
            Completion = new Task(
                () => { }
                );
        }

        internal override void LinkBuffers(DataFlowComponent successor, LinkPredicates linkPredicate)
        {
            var s = successor as IDataFlowDestination<ETLBoxError>;
            var lp = new BufferLinker<ETLBoxError>(linkPredicate);
            lp.LinkBlocksWithPredicates(SourceBlock, s.TargetBlock);
        }

        /// <summary>
        /// Sends the error message into the error data flow
        /// </summary>
        /// <param name="e">The exception message</param>
        /// <param name="jsonRow">The serialized erroneous row</param>
        public void Send(Exception e, string jsonRow)
        {
            if (Redirection != null) Redirection.Send(e, jsonRow);
            else
            {
                if (!Buffer.SendAsync(new ETLBoxError()
                {
                    ExceptionType = e.GetType().ToString(),
                    ErrorText = e.Message,
                    ReportTime = DateTime.Now,
                    RecordAsJson = jsonRow
                }).Result)
                    throw this.Exception;
            }
        }

        /// <summary>
        /// Serialized a row using the default json serialization
        /// </summary>
        /// <typeparam name="T">Type of the row</typeparam>
        /// <param name="row">The errorneous row</param>
        /// <returns>The faulty row serialized as json</returns>
        public static string ConvertErrorData<T>(T row)
        {
            try
            {
                return JsonConvert.SerializeObject(row, new JsonSerializerSettings());
            }
            catch (Exception e)
            {
                return e.Message;
            }
        }
    }
}
