using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using ETLBox.Helper;

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

        #endregion

        #region Internal properties

        protected string CurrentRequestUri { get; set; }
        protected StreamReader StreamReader { get; set; }
        protected StringBuilder UnparsedData { get; set; }

        #endregion

        #region Implement abstract methods

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

        protected override void CleanUpOnFaulted(Exception e) {
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
            if (ResourceType == ResourceType.File)
                StreamReader = new StreamReader(uri);
            else
            {
                var message = HttpRequestMessage.Clone();
                message.RequestUri =  new Uri(uri);
                var response = HttpClient.SendAsync(message, HttpCompletionOption.ResponseHeadersRead).Result;
                response.EnsureSuccessStatusCode();
                StreamReader = new StreamReader(response.Content.ReadAsStreamAsync().Result);
            }
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
