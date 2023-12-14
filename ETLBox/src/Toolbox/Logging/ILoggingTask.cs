using Microsoft.Extensions.Logging;

namespace ALE.ETLBox.src.Toolbox.Logging
{
    public interface ILoggingTask
    {
        ILogger Logger { get; set; }
    }
}
