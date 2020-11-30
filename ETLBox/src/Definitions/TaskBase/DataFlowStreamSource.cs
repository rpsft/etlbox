using ETLBox.Helper;
using System;
using System.IO;
using System.Net.Http;
using System.Text;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowStreamSource<TOutput> : DataFlowExecutableSource<TOutput>, IDataFlowStreamSource<TOutput>
    {
        #region Public properties

        /// <inheritdoc/>
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
        private string _uri;

        /// <inheritdoc/>
        public Func<StreamMetaData, string> GetNextUri { get; set; }

        /// <inheritdoc/>
        public Func<StreamMetaData, bool> HasNextUri { get; set; }

        /// <inheritdoc/>
        public ResourceType ResourceType { get; set; }

        /// <inheritdoc/>
        public HttpClient HttpClient { get; set; } = new HttpClient();

        /// <inheritdoc/>
        public HttpRequestMessage HttpRequestMessage { get; set; } = new HttpRequestMessage();

        /// <summary>
        /// Number of rows to skip before starting reading the header and csv data
        /// </summary>
        public int SkipRows { get; set; } = 0;

        /// <summary>
        /// Encoding used to read data from the source file or web request. 
        /// </summary>
        public Encoding Encoding { get; set; }

        #endregion

        #region Internal properties

        protected string CurrentRequestUri { get; set; }
        protected StreamReader StreamReader { get; set; }
        protected StringBuilder UnparsedData { get; set; }

        #endregion

        #region Implement abstract methods

        protected override void CheckParameter() {  }

        protected override void OnExecutionDoSynchronousWork()
        {

        }

        protected override void OnExecutionDoAsyncWork()
        {
            NLogStartOnce();
            do
            {
                CurrentRequestUri = GetNextUri(CreateMetaDataObject);
                OpenStream(CurrentRequestUri);
                InitReader();
                WasStreamOpened = true;
                ReadAllRecords();
            } while (HasNextUri(CreateMetaDataObject));
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
            CloseStreamsIfOpen();
        }

        protected override void CleanUpOnFaulted(Exception e)
        {
            CloseStreamsIfOpen();
        }

        #endregion

        #region Implementation

        private bool WasStreamOpened;

        private void CloseStreamsIfOpen()
        {
            if (WasStreamOpened)
            {
                CloseReader();
                CloseStream();
            }
        }

        private StreamMetaData CreateMetaDataObject =>
                new StreamMetaData()
                {
                    ProgressCount = ProgressCount,
                    UnparsedData = UnparsedData?.ToString()
                };


        private void OpenStream(string uri)
        {
            if (ResourceType == ResourceType.File) { 
                if (Encoding == null)
                    StreamReader = new StreamReader(uri,true);
                else
                    StreamReader = new StreamReader(uri, Encoding);
            }
            else
            {
                var message = HttpRequestMessage.Clone();
                message.RequestUri = new Uri(uri);
                var response = HttpClient.SendAsync(message, HttpCompletionOption.ResponseHeadersRead).Result;
                response.EnsureSuccessStatusCode();
                if (Encoding == null)
                    StreamReader = new StreamReader(response.Content.ReadAsStreamAsync().Result, true);
                else
                    StreamReader = new StreamReader(response.Content.ReadAsStreamAsync().Result, Encoding);
            }
            SkipFirstRows();
        }

        private void SkipFirstRows()
        {
            for (int i = 0; i < SkipRows; i++)
                StreamReader.ReadLine();
        }

        private void CloseStream()
        {
            HttpClient?.Dispose();
            StreamReader?.Dispose();
        }

        protected abstract void InitReader();
        protected abstract void ReadAllRecords();
        protected abstract void CloseReader();

        #endregion
    }
}
