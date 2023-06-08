using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace ALE.ETLBox.Helper
{
    internal class PushStreamContent : HttpContent
    {
        private readonly Func<Stream, HttpContent, TransportContext, Task> _onStreamAvailable;

        public PushStreamContent(Action<Stream, HttpContent, TransportContext> onStreamAvailable)
            : this(AsTask(onStreamAvailable), (MediaTypeHeaderValue)null) { }

        public PushStreamContent(
            Func<Stream, HttpContent, TransportContext, Task> onStreamAvailable
        )
            : this(onStreamAvailable, (MediaTypeHeaderValue)null) { }

        public PushStreamContent(
            Action<Stream, HttpContent, TransportContext> onStreamAvailable,
            string mediaType
        )
            : this(AsTask(onStreamAvailable), new MediaTypeHeaderValue(mediaType)) { }

        public PushStreamContent(
            Func<Stream, HttpContent, TransportContext, Task> onStreamAvailable,
            string mediaType
        )
            : this(onStreamAvailable, new MediaTypeHeaderValue(mediaType)) { }

        public PushStreamContent(
            Action<Stream, HttpContent, TransportContext> onStreamAvailable,
            MediaTypeHeaderValue mediaType
        )
            : this(AsTask(onStreamAvailable), mediaType) { }

        public PushStreamContent(
            Func<Stream, HttpContent, TransportContext, Task> onStreamAvailable,
            MediaTypeHeaderValue mediaType
        )
        {
            _onStreamAvailable =
                onStreamAvailable ?? throw new ArgumentException(nameof(onStreamAvailable));
            Headers.ContentType = mediaType ?? ApplicationOctetStreamMediaType;
        }

        private MediaTypeHeaderValue ApplicationOctetStreamMediaType =>
            new("application/octet-stream");

        private static Func<Stream, HttpContent, TransportContext, Task> AsTask(
            Action<Stream, HttpContent, TransportContext> onStreamAvailable
        )
        {
            if (onStreamAvailable == null)
                throw new ArgumentException(nameof(onStreamAvailable));
            return (stream, content, transportContext) =>
            {
                onStreamAvailable(stream, content, transportContext);
                return FromResult(new AsyncVoid());
            };
        }

        public static Task<TResult> FromResult<TResult>(TResult result)
        {
            var completionSource = new TaskCompletionSource<TResult>();
            completionSource.SetResult(result);
            return completionSource.Task;
        }

        protected override async Task SerializeToStreamAsync(
            Stream stream,
            TransportContext context
        )
        {
            var pushStreamContent = this;
            var serializeToStreamTask = new TaskCompletionSource<bool>();
            Stream stream1 = new CompleteTaskOnCloseStream(stream, serializeToStreamTask);
            await pushStreamContent._onStreamAvailable(stream1, pushStreamContent, context);
            await serializeToStreamTask.Task;
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1L;
            return false;
        }

        [StructLayout(LayoutKind.Sequential, Size = 1)]
        private struct AsyncVoid { }

        internal class CompleteTaskOnCloseStream : DelegatingStream
        {
            private readonly TaskCompletionSource<bool> _serializeToStreamTask;

            public CompleteTaskOnCloseStream(
                Stream innerStream,
                TaskCompletionSource<bool> serializeToStreamTask
            )
                : base(innerStream)
            {
                _serializeToStreamTask =
                    serializeToStreamTask
                    ?? throw new ArgumentNullException(nameof(serializeToStreamTask));
            }

            protected override void Dispose(bool disposing)
            {
                _serializeToStreamTask.TrySetResult(true);
            }

            public override void Close()
            {
                _serializeToStreamTask.TrySetResult(true);
            }
        }
    }
}
