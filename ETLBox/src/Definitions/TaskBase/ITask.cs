using ALE.ETLBox.src.Definitions.ConnectionManager;

namespace ALE.ETLBox.src.Definitions.TaskBase
{
    [PublicAPI]
    public interface ITask
    {
        string TaskName { get; }
        string TaskType { get; }
        string TaskHash { get; }
        IConnectionManager ConnectionManager { get; }
        bool DisableLogging { get; }
        CultureInfo CurrentCulture { get; }
    }
}
