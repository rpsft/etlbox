using System.Globalization;

namespace ETLBox.Primitives
{
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
