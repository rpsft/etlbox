using System.Globalization;
using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox
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
