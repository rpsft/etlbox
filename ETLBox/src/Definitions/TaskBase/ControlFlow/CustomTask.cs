﻿using System.Diagnostics.CodeAnalysis;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// A custom task allows you to run your own code (defined as an Action object), with additionally logging in place. (TaskType: CUSTOM)
    /// </summary>
    [PublicAPI]
    [SuppressMessage("ReSharper", "TemplateIsNotCompileTimeConstantProblem")]
    public class CustomTask : GenericTask
    {
        /* ITask interface */
        public sealed override string TaskName { get; set; }

        public void Execute()
        {
            throw new Exception("A custom task can't be used without an Action!");
        }

        public CustomTask(string name)
        {
            TaskName = name;
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

        public static void Execute(string name, Action task) => new CustomTask(name).Execute(task);

        public static void Execute<t1>(string name, Action<t1> task, t1 param1) =>
            new CustomTask(name).Execute(task, param1);

        public static void Execute<t1, t2>(
            string name,
            Action<t1, t2> task,
            t1 param1,
            t2 param2
        ) => new CustomTask(name).Execute(task, param1, param2);

        private void NLogStart()
        {
            if (!DisableLogging)
                NLogger.Info(
                    TaskName,
                    TaskType,
                    "START",
                    TaskHash,
                    ControlFlow.STAGE,
                    ControlFlow.CurrentLoadProcess?.Id
                );
        }

        private void NLogFinish()
        {
            if (!DisableLogging)
                NLogger.Info(
                    TaskName,
                    TaskType,
                    "END",
                    TaskHash,
                    ControlFlow.STAGE,
                    ControlFlow.CurrentLoadProcess?.Id
                );
        }
    }
}
