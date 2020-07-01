using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowStreamSource<TOutput> : DataFlowSource<TOutput>
    {
        /* Public properties */
        /// <summary>
        /// The Url of the webservice (e.g. https://test.com/foo) or the file name (relative or absolute)
        /// </summary>
        public string Uri
        {
            get
            {
                return _uri;
            }
            set
            {
                _uri = value;
                GetNextUri = c => _uri;
                HasNextUri = c => false;
            }
        }

        public Func<StreamMetaData, string> GetNextUri { get; set; }
        public Func<StreamMetaData, bool> HasNextUri { get; set; }

        /// <summary>
        /// Specifies the resource type. By default requests are made with HttpClient.
        /// Specify ResourceType.File if you want to read from a json file.
        /// </summary>
        public ResourceType ResourceType { get; set; }

        public HttpClient HttpClient { get; set; } = new HttpClient();

        /* Internal properties */
        protected string _uri;
        protected string CurrentRequestUri { get; set; }
        protected StreamReader StreamReader { get; set; }
        private bool WasStreamOpened { get; set; }
        protected StringBuilder UnparsedData { get; set; }

        private StreamMetaData CreateMetaDataObject =>
                new StreamMetaData()
                {
                    ProgressCount = ProgressCount,
                    UnparsedData = UnparsedData?.ToString()
                };

        public override void Execute()
        {
            NLogStart();
            try
            {
                do
                {
                    CurrentRequestUri = GetNextUri(CreateMetaDataObject);
                    OpenStream(CurrentRequestUri);
                    InitReader();
                    WasStreamOpened = true;
                    ReadAll();
                } while (HasNextUri(CreateMetaDataObject));
                Buffer.Complete();
            }
            finally
            {
                if (WasStreamOpened)
                {
                    CloseReader();
                    CloseStream();
                }
            }
            NLogFinish();
        }

        private void OpenStream(string uri)
        {
            if (ResourceType == ResourceType.File)
                StreamReader = new StreamReader(uri);
            else
                StreamReader = new StreamReader(HttpClient.GetStreamAsync(new Uri(uri)).Result);
        }

        private void CloseStream()
        {
            HttpClient?.Dispose();
            StreamReader?.Dispose();
        }

        protected abstract void InitReader();
        protected abstract void ReadAll();
        protected abstract void CloseReader();
    }

    public class StreamMetaData
    {
        public int ProgressCount { get; set; }
        public string UnparsedData { get; set; }
    }
}
