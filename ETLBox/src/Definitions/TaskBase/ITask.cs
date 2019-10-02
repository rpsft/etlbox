using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox {
    public interface ITask
    {        
        string TaskName { get; }
        string TaskType { get; }
        string TaskHash { get; }        
        IConnectionManager ConnectionManager { get; }
        bool DisableLogging { get; }
        void Execute();
    }
}
