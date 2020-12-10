using ETLBox.ControlFlow;
using System;

namespace ETLBox.Logging
{
    /// <summary>
    /// A custom task allows you to run your own code (defined as an Action object), with additionally logging in place. (TaskType: CUSTOM)
    /// </summary>
    public class CustomTask : ControlFlowTask
    {
        /// <inheritdoc/>
        public override string TaskName { get; set; }

        public void Execute()
        {
            throw new Exception("A custom task can't be used without an Action!");
        }

        public CustomTask(string name)
        {
            this.TaskName = name;
        }


        public void Execute(Action task)
        {
            NLogStart();
            task.Invoke();
            NLogFinish();
        }

        public void Execute<t1>(Action<t1> task, t1 param1)
        {
            NLogStart();
            task.Invoke(param1);
            NLogFinish();
        }

        public void Execute<t1, t2>(Action<t1, t2> task, t1 param1, t2 param2)
        {
            NLogStart();
            task.Invoke(param1, param2);
            NLogFinish();
        }

        public static void Execute(string name, Action task) =>
           new CustomTask(name).Execute(task);

        public static void Execute<t1>(string name, Action<t1> task, t1 param1) =>
           new CustomTask(name).Execute<t1>(task, param1);

        public static void Execute<t1, t2>(string name, Action<t1, t2> task, t1 param1, t2 param2) =>
            new CustomTask(name).Execute<t1, t2>(task, param1, param2);


        void NLogStart()
        {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "START", TaskHash, Logging.STAGE, Logging.CurrentLoadProcess?.Id);
        }

        void NLogFinish()
        {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "END", TaskHash, Logging.STAGE, Logging.CurrentLoadProcess?.Id);
        }


    }
}