using ETLBox.ControlFlow;
using System;

namespace ETLBox.Logging
{
    /// <summary>
    /// A package is a shortcute for custom task, but with the TaskType "PACKAGE".
    /// </summary>
    public class Package : ControlFlowTask
    {
        public override string TaskName { get; set; } = "Package";
        public void Execute() => new CustomTask(TaskName) { TaskType = this.TaskType, TaskHash = this.TaskHash }.Execute(Tasks);
        public Action Tasks { get; set; }

        public Package() { }

        public Package(string name) : this()
        {
            TaskName = name;
        }

        public Package(string name, Action tasks) : this(name)
        {
            this.Tasks = tasks;
        }

        public static void Execute(string name, Action tasks) => new Package(name, tasks).Execute();
    }
}
