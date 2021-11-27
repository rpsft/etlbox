﻿using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using ETLBox.Helper;

namespace ALE.ETLBox.DataFlow;

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
            var pushStreamContent = new PushStreamContent(async (stream, ct, tc) =>
            {
                try
                {
                    StreamWriter = Encoding != null ? new StreamWriter(stream, Encoding) : new StreamWriter(stream);
                    CanWriteCompletionSource.SetResult(true);
                }
                catch (Exception ex)
                {
                    CanWriteCompletionSource?.SetException(ex);
                }

                await DoneWritingCompletionSource?.Task;
                stream?.Close();
            }, HttpContentType);
            request.Content = pushStreamContent;
            HttpResponseMessage = HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead,
                BufferCancellationSource.Token);
        }
        // StreamWriter = new StreamWriter(HttpClient.GetStreamAsync(new Uri(Uri)).Result);
    }

    protected override void CleanUp()
    {
        CloseStream();
        
        StreamWriter?.Close();
        
        if (ResourceType != ResourceType.Http)
            return;
        
        DoneWritingCompletionSource?.SetResult(true);
        
        if (HttpResponseMessage == null)
            return;
        HttpResponseMessage?.Result?.EnsureSuccessStatusCode();
        HttpResponseMessage?.Dispose();
        
        OnCompletion?.Invoke();
        
        NLogFinish();
    }

    protected abstract void InitStream();
    protected abstract void WriteIntoStream(TInput data);
    protected abstract void CloseStream();
}