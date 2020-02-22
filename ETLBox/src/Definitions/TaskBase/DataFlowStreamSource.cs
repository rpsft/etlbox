using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public abstract class DataFlowStreamSource<TOutput> : DataFlowSource<TOutput>
    {
        /* Public properties */
        /// <summary>
        /// The Url of the webservice (e.g. https://test.com/foo) or the file name (relative or absolute)
        /// </summary>
        public string Uri { get; set; }
        /// <summary>
        /// Specifies the resourc type. By default requests are made with HttpClient.
        /// Specify ResourceType.File if you want to read from a json file.
        /// </summary>
        public ResourceType ResourceType { get; set; }

        protected StreamReader StreamReader { get; set; }
        public HttpClient HttpClient { get; set; } = new HttpClient();

        public override void Execute()
        {
            NLogStart();
            OpenStream();
            InitReader();
            try
            {
                ReadAll();
                Buffer.Complete();
            }
            finally
            {
                CloseReader();
                CloseStream();
            }
            NLogFinish();
        }

        protected virtual void OpenStream()
        {
            if (ResourceType == ResourceType.File)
                StreamReader = new StreamReader(Uri, true);
            else
                StreamReader = new StreamReader(HttpClient.GetStreamAsync(new Uri(Uri)).Result);
        }

        protected virtual void CloseStream()
        {
            StreamReader?.Dispose();
            HttpClient?.Dispose();
        }

        protected abstract void InitReader();
        protected abstract void ReadAll();
        protected abstract void CloseReader();

    }
}
