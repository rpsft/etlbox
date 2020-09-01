using ETLBox.Connection;

namespace ETLBox.ControlFlow
{
    public interface ILoggableTask
    {
        string TaskName { get; set; }
        string TaskType { get; set; }
        string TaskHash { get; set; }
        //IConnectionManager ConnectionManager { get; set; }
        bool DisableLogging { get; set; }
    }
}
