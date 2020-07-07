using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowStreamDestination<TInput> : DataFlowDestination<TInput>
    {
        /* Public properties */
        /// <summary>
        /// The Url of the webservice (e.g. https://test.com/foo) or the file name (relative or absolute)
        /// </summary>
        public string Uri { get; set; }

        /// <summary>
        /// Specifies the resourc type. ResourceType.
        /// Specify ResourceType.File if you want to write into a file.
        /// </summary>
        public ResourceType ResourceType { get; set; }

        protected StreamWriter StreamWriter { get; set; }
        public HttpClient HttpClient { get; set; } = new HttpClient();

        protected void InitTargetAction()
        {
            InitBufferObjects();
        }

        protected override void InitBufferObjects()
        {
            TargetAction = new ActionBlock<TInput>(WriteData, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = 1,
                BoundedCapacity = MaxBufferSize
            });
            SetCompletionTask();
        }

        protected void WriteData(TInput data)
        {
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

        protected override void CleanUp()
        {
            CloseStream();
            StreamWriter?.Close();
            OnCompletion?.Invoke();
            NLogFinish();
        }

        protected abstract void InitStream();
        protected abstract void WriteIntoStream(TInput data);
        protected abstract void CloseStream();
    }
}
