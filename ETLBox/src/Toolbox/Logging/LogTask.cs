﻿using System.Diagnostics.CodeAnalysis;
using ALE.ETLBox.Common;
using ALE.ETLBox.Common.ControlFlow;
using ETLBox.Primitives;

namespace ALE.ETLBox.Logging
{
    /// <summary>
    /// Used this task for custom log messages.
    /// </summary>
    [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
    [SuppressMessage("ReSharper", "TemplateIsNotCompileTimeConstantProblem")]
    public class LogTask : GenericTask
    {
        /* ITask Interface */
        public override string TaskName => "Logs message";

        public void Execute()
        {
            Info(Message);
        }

        /* Public properties */
        public string Message { get; set; }

        public LogTask() { }

        public LogTask(string message)
            : this()
        {
            Message = message;
        }

        public static void Trace(string message) => new LogTask(message).Trace();

        public static void Trace(IConnectionManager connectionManager, string message) =>
            new LogTask(message) { ConnectionManager = connectionManager }.Trace();

        public void Trace() =>
            NLogger?.Trace(
                Message,
                TaskType,
                "LOG",
                TaskHash,
                Common.ControlFlow.ControlFlow.Stage,
                Common.ControlFlow.ControlFlow.CurrentLoadProcess?.Id
            );

        public static void Debug(string message) => new LogTask(message).Debug();

        public static void Debug(IConnectionManager connectionManager, string message) =>
            new LogTask(message) { ConnectionManager = connectionManager }.Debug();

        public void Debug() =>
            NLogger?.Debug(
                Message,
                TaskType,
                "LOG",
                TaskHash,
                Common.ControlFlow.ControlFlow.Stage,
                Common.ControlFlow.ControlFlow.CurrentLoadProcess?.Id
            );

        public static void Info(string message) => new LogTask(message).Info();

        public static void Info(IConnectionManager connectionManager, string message) =>
            new LogTask(message) { ConnectionManager = connectionManager }.Info();

        public void Info() =>
            NLogger?.Info(
                Message,
                TaskType,
                "LOG",
                TaskHash,
                Common.ControlFlow.ControlFlow.Stage,
                Common.ControlFlow.ControlFlow.CurrentLoadProcess?.Id
            );

        public static void Warn(string message) => new LogTask(message).Warn();

        public static void Warn(IConnectionManager connectionManager, string message) =>
            new LogTask(message) { ConnectionManager = connectionManager }.Warn();

        public void Warn() =>
            NLogger?.Warn(
                Message,
                TaskType,
                "LOG",
                TaskHash,
                Common.ControlFlow.ControlFlow.Stage,
                Common.ControlFlow.ControlFlow.CurrentLoadProcess?.Id
            );

        public static void Error(string message) => new LogTask(message).Error();

        public static void Error(IConnectionManager connectionManager, string message) =>
            new LogTask(message) { ConnectionManager = connectionManager }.Error();

        public void Error() =>
            NLogger?.Error(
                Message,
                TaskType,
                "LOG",
                TaskHash,
                Common.ControlFlow.ControlFlow.Stage,
                Common.ControlFlow.ControlFlow.CurrentLoadProcess?.Id
            );

        public static void Fatal(string message) => new LogTask(message).Fatal();

        public static void Fatal(IConnectionManager connectionManager, string message) =>
            new LogTask(message) { ConnectionManager = connectionManager }.Fatal();

        public void Fatal() =>
            NLogger?.Fatal(
                Message,
                TaskType,
                "LOG",
                TaskHash,
                Common.ControlFlow.ControlFlow.Stage,
                Common.ControlFlow.ControlFlow.CurrentLoadProcess?.Id
            );
    }
}
