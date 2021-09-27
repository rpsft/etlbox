using ETLBox.Connection;
using System;

namespace ETLBox.Logging
{
    public class LoadProcess
    {
        public long? Id { get; set; }
        public DateTime StartDate { get; set; }
        public DateTime? EndDate { get; set; }
        public string Source { get; set; }
        public long? SourceId { get; set; }
        public string ProcessName { get; set; }
        public string StartMessage { get; set; }
        public bool IsRunning { get; set; }
        public string EndMessage { get; set; }
        public bool WasSuccessful { get; set; }
        public string AbortMessage { get; set; }
        public bool WasAborted { get; set; }
        public bool IsFinished => WasSuccessful || WasAborted;

        public LoadProcess End() => LoadProcessTask.End(this);
        public LoadProcess End(string message) => LoadProcessTask.End(this, message);
        public LoadProcess End(IConnectionManager connection) => LoadProcessTask.End(connection, this);
        public LoadProcess End(IConnectionManager connection, string message) => LoadProcessTask.End(connection, this, message);
        public LoadProcess Abort() => LoadProcessTask.Abort(this);
        public LoadProcess Abort(string message) => LoadProcessTask.Abort(this, message);
        public LoadProcess Abort(IConnectionManager connection) => LoadProcessTask.Abort(connection, this);
        public LoadProcess Abort(IConnectionManager connection, string message) => LoadProcessTask.Abort(connection, this, message);

    }
}
