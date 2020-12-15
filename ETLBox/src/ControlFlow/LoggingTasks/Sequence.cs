using ETLBox.ControlFlow;
using System;

namespace ETLBox.Logging
{
    /// <summary>
    /// A sequence is a shortcute for custom task, but with the TaskType "SEQUENCE".
    /// </summary>
    public class Sequence : ControlFlowTask
    {
        public override string TaskName { get; set; } = "Sequence";
        public void Execute() => new CustomTask(TaskName) { TaskType = this.TaskType, TaskHash = this.TaskHash }.Execute(Tasks);
        public Action Tasks { get; set; }

        public Sequence()
        { }

        public Sequence(string name) : this()
        {
            TaskName = name;
        }

        public Sequence(string name, Action tasks) : this(name)
        {
            this.Tasks = tasks;
        }

        public static void Execute(string name, Action tasks) => new Sequence(name, tasks).Execute();
    }

    public class Sequence<T> : Sequence
    {
        public T Parent { get; set; }
        public new Action<T> Tasks { get; set; }
        public Sequence() : base()
        { }

        public Sequence(string name) : base(name) { }
        public Sequence(string name, Action<T> tasks, T parent) : base(name)
        {
            this.Tasks = tasks;
            this.Parent = parent;
        }

        public new void Execute() => new CustomTask(TaskName) { TaskType = this.TaskType, TaskHash = this.TaskHash }.Execute(Tasks, Parent);

        public static void Execute(string name, Action<T> tasks, T parent) => new Sequence<T>(name, tasks, parent).Execute();
    }
}
