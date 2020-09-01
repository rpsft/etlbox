using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// The base implementation for a destination that allows writing of data in a stream.
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data</typeparam>
    public abstract class DataFlowStreamDestination<TInput> : DataFlowDestination<TInput>, IDataFlowStreamDestination<TInput>
    {
        #region Public properties

        /// <inheritdoc/>
        public string Uri { get; set; }

        /// <inheritdoc/>
        public ResourceType ResourceType { get; set; }

        /// <inheritdoc/>
        public HttpClient HttpClient { get; set; } = new HttpClient();

        #endregion

        #region Implement abstract methods

        protected override void InternalInitBufferObjects()
        {
            TargetAction = new ActionBlock<TInput>(WriteData, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = 1,
                BoundedCapacity = MaxBufferSize,
            });
        }

        protected override void CleanUpOnSuccess()
        {
            CloseStream();
            StreamWriter?.Close();
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e)
        {
            CloseStream();
            StreamWriter?.Close();
        }

        #endregion

        #region Implementation template

        protected StreamWriter StreamWriter { get; set; }

        protected void WriteData(TInput data)
        {
            NLogStartOnce();
            if (StreamWriter == null)
            {
                CreateStreamWriterByResourceType();
                InitStream();
            }
            WriteIntoStream(data);
        }

        private void CreateStreamWriterByResourceType()
        {
            if (ResourceType == ResourceType.File)
                StreamWriter = new StreamWriter(Uri);
            else
                StreamWriter = new StreamWriter(HttpClient.GetStreamAsync(new Uri(Uri)).Result);
        }

        protected abstract void InitStream();
        protected abstract void WriteIntoStream(TInput data);
        protected abstract void CloseStream();

        #endregion
    }
}
