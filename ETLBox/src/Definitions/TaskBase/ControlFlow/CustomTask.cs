using System.Diagnostics.CodeAnalysis;
using ALE.ETLBox.src.Definitions.TaskBase;
using ALE.ETLBox.src.Toolbox.ControlFlow;

namespace ALE.ETLBox.src.Definitions.TaskBase.ControlFlow
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

        public CustomTask(string name)
        {
            TaskName = name;
        }

        public static void Execute()
        {
            throw new InvalidOperationException("A custom task can't be used without an Action!");
        }

        public void Execute(Action task)
        {
            LogStart();
            task.Invoke();
            LogFinish();
        }

        public void Execute<T1>(Action<T1> task, T1 param1)
        {
            LogStart();
            task.Invoke(param1);
            LogFinish();
        }

        public void Execute<T1, T2>(Action<T1, T2> task, T1 param1, T2 param2)
        {
            LogStart();
            task.Invoke(param1, param2);
            LogFinish();
        }

        public static void Execute(string name, Action task) => new CustomTask(name).Execute(task);

        public static void Execute<T1>(string name, Action<T1> task, T1 param1) =>
            new CustomTask(name).Execute(task, param1);

        public static void Execute<T1, T2>(
            string name,
            Action<T1, T2> task,
            T1 param1,
            T2 param2
        ) => new CustomTask(name).Execute(task, param1, param2);

        private void LogStart()
        {
            if (!DisableLogging)
                Logger.Info(
                    TaskName,
                    TaskType,
                    "START",
                    TaskHash,
                    Toolbox.ControlFlow.ControlFlow.Stage,
                    Toolbox.ControlFlow.ControlFlow.CurrentLoadProcess?.Id
                );
        }

        private void LogFinish()
        {
            if (!DisableLogging)
                Logger.Info(
                    TaskName,
                    TaskType,
                    "END",
                    TaskHash,
                    Toolbox.ControlFlow.ControlFlow.Stage,
                    Toolbox.ControlFlow.ControlFlow.CurrentLoadProcess?.Id
                );
        }
    }
}
