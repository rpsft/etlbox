using ETLBox.Connection;

namespace ETLBox
{
    public interface ITask
    {
        string TaskName { get; set; }
        string TaskType { get; set; }
        string TaskHash { get; set; }
        IConnectionManager ConnectionManager { get; set; }
        bool DisableLogging { get; set; }
    }
}
