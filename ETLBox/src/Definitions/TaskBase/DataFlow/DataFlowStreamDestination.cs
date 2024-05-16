using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.Helper;

namespace ALE.ETLBox.DataFlow
{
    [PublicAPI]
    public abstract class DataFlowStreamDestination<TInput> : DataFlowDestination<TInput>
    {
        /* Public properties */
        /// <summary>
        ///   The Url of the webservice (e.g. https://test.com/foo) or the file name (relative or absolute)
        /// </summary>
        public string Uri { get; set; }

        /// <summary>
        ///   Specifies the resource type. ResourceType.
        ///   Specify ResourceType.File if you want to write into a file.
        /// </summary>
        public ResourceType ResourceType { get; set; }

        protected StreamWriter StreamWriter { get; set; }

        public HttpClient HttpClient { get; set; } = new();

        internal CancellationTokenSource BufferCancellationSource { get; set; } = new();

        public Task<HttpResponseMessage> HttpResponseMessage { get; set; }

        public string HttpContentType { get; set; } = "text/plain";

        public Encoding Encoding { get; set; }

        public HttpRequestMessage HttpRequestMessage { get; set; } = new(HttpMethod.Post, "");

        private TaskCompletionSource<bool> DoneWritingCompletionSource { get; set; }

        private TaskCompletionSource<bool> CanWriteCompletionSource { get; set; }

        protected void InitTargetAction()
        {
            TargetAction = new ActionBlock<TInput>(WriteData);
            SetCompletionTask();
        }

        protected void WriteData(TInput data)
        {
            if (data == null)
                return;

            if (StreamWriter == null)
            {
                CreateStreamWriterByResourceType(Uri);
                CanWriteCompletionSource?.Task.Wait();
                InitStream();
            }

            WriteIntoStream(data);
        }

        private void CreateStreamWriterByResourceType(string uri)
        {
            if (ResourceType == ResourceType.File)
            {
                StreamWriter = new StreamWriter(uri);
            }
            else
            {
                CanWriteCompletionSource = new TaskCompletionSource<bool>();
                DoneWritingCompletionSource = new TaskCompletionSource<bool>();
                var request = HttpRequestMessage.Clone();
                request.RequestUri = new Uri(Uri);
                var pushStreamContent = new PushStreamContent(
                    async (stream, _, _) =>
                    {
                        try
                        {
                            StreamWriter =
                                Encoding != null
                                    ? new StreamWriter(stream, Encoding)
                                    : new StreamWriter(stream);
                            CanWriteCompletionSource.SetResult(true);
                        }
                        catch (Exception ex)
                        {
                            CanWriteCompletionSource?.SetException(ex);
                        }

                        await DoneWritingCompletionSource.Task;
                        stream?.Close();
                    },
                    HttpContentType
                );
                request.Content = pushStreamContent;
                HttpResponseMessage = HttpClient.SendAsync(
                    request,
                    HttpCompletionOption.ResponseHeadersRead,
                    BufferCancellationSource.Token
                );
            }
        }

        protected override void CleanUp()
        {
            CloseStream();

            StreamWriter?.Close();

            if (ResourceType == ResourceType.Http)
            {
                DoneWritingCompletionSource?.SetResult(true);

                HttpResponseMessage?.Result?.EnsureSuccessStatusCode();
                HttpResponseMessage?.Dispose();
            }

            OnCompletion?.Invoke();

            LogFinish();
        }

        protected abstract void InitStream();
        protected abstract void WriteIntoStream(TInput data);
        protected abstract void CloseStream();
    }
}
