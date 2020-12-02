using ETLBox.Exceptions;
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
    public sealed class ErrorSource : DataFlowExecutableSource<ETLBoxError>
    {
        /// <summary>
        /// If set to another error source, all message send to this source will redirected.
        /// </summary>
        public ErrorSource Redirection { get; set; }

        public ErrorSource()
        {
          
        }
        
        protected override void CheckParameter() { }
                
        protected override void InitComponent()
        {
            base.InitComponent();
            SourceOrPredecessorCompletion = SourceTask;         
        }

        //The complete is called explicit in the DataFlowComponent
        protected override bool CompleteManually { get; set; } = true;

        protected override void OnExecutionDoSynchronousWork() { } 

        protected override void OnExecutionDoAsyncWork() { } 

        internal override void LinkBuffers(DataFlowComponent successor, LinkPredicates linkPredicate)
        {
            var s = successor as IDataFlowDestination<ETLBoxError>;
            var lp = new BufferLinker<ETLBoxError>(linkPredicate);
            lp.LinkBlocksWithPredicates(SourceBlock, s.TargetBlock);
        }

        /// Called from DataFlowComponent - after network init the error source are
        /// startet with manual completion
        internal void LetErrorSourceWaitForInput() => this.ExecuteAsync();

        /// This is called when a component completes
        internal void LetErrorSourceFinishUp() => this.CompleteBuffer();

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
                    throw new ETLBoxFaultedBufferException("This was not supposed to happen - " +
                        "the error buffer faulted & no data " +
                        "can be written into the error output anymore.",e);
            }
        }

        /// <summary>
        /// Serialized a row using the default json serialization
        /// </summary>
        /// <typeparam name="T">Type of the row</typeparam>
        /// <param name="row">The errorneous row</param>
        /// <returns>The faulty row serialized as json or "null" if input is null</returns>
        public static string ConvertErrorData<T>(T row)
        {
            try
            {
                if (row == null) return "null";
                return JsonConvert.SerializeObject(row, new JsonSerializerSettings());
            }
            catch (Exception e)
            {
                return e.Message;
            }
        }
    }
}
