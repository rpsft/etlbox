namespace ETLBox.ControlFlow
{
    public interface ILoggableTask
    {
        /// <summary>
        /// A name to identify the task or component. Every component or task comes
        /// with a default name that can be overwritten.
        /// </summary>
        string TaskName { get; set; }

        /// <summary>
        /// A type description of the task or component. This is usually the class name.
        /// </summary>
        string TaskType { get; set; }

        /// <summary>
        /// Creates a unique hash value to identify the task or component.
        /// </summary>
        string TaskHash { get; set; }

        /// <summary>
        /// If set to true, the component or task won't produce any log output.
        /// </summary>
        bool DisableLogging { get; set; }
    }
}
