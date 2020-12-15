using ETLBox.Connection;
using System;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// This task can exeucte any XMLA.
    /// </summary>
    /// <example>
    /// <code>
    /// XmlaTask.ExecuteNonQuery("Log description here","Xmla goes here...")
    /// </code>
    /// </example>
    public class XmlaTask : DbTask
    {
        public override string TaskName { get; set; } = "Run some xmla";

        public XmlaTask()
        {
            Init();
        }

        public XmlaTask(string xmla) : base(xmla)
        {
            Init();
        }

        internal XmlaTask(ControlFlowTask callingTask, string xmla) : base(callingTask, xmla)
        {
            Init();
        }

        public XmlaTask(string name, string xmla) : base(name, xmla)
        {
            Init();
        }

        public XmlaTask(string xmla, params Action<object>[] actions) : base(xmla, actions)
        {
            Init();
        }

        public XmlaTask(string xmla, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) : base(xmla, beforeRowReadAction, afterRowReadAction, actions)
        {
            Init();
        }

        private void Init()
        {
            DoXMLCommentStyle = true;
        }

        /* Static methods for convenience */
        public static int ExecuteNonQuery(string xmla) => new XmlaTask(xmla).ExecuteNonQuery();
        public static int ExecuteNonQuery(string name, string xmla) => new XmlaTask(name, xmla).ExecuteNonQuery();
        public static object ExecuteScalar(string xmla) => new XmlaTask(xmla).ExecuteScalar();
        public static object ExecuteScalar(string name, string xmla) => new XmlaTask(name, xmla).ExecuteScalar();
        public static Nullable<T> ExecuteScalar<T>(string xmla) where T : struct => new XmlaTask(xmla).ExecuteScalar<T>();
        public static Nullable<T> ExecuteScalar<T>(string name, string xmla) where T : struct => new XmlaTask(name, xmla).ExecuteScalar<T>();
        public static bool ExecuteScalarAsBool(string xmla) => new XmlaTask(xmla).ExecuteScalarAsBool();
        public static bool ExecuteScalarAsBool(string name, string xmla) => new XmlaTask(name, xmla).ExecuteScalarAsBool();
        public static void ExecuteReader(string xmla, params Action<object>[] actions) => new XmlaTask(xmla, actions).ExecuteReader();
        public static void ExecuteReader(string xmla, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) =>
            new XmlaTask(xmla, beforeRowReadAction, afterRowReadAction, actions).ExecuteReader();
        public static int ExecuteNonQuery(IConnectionManager connectionManager, string xmla) => new XmlaTask(xmla) { ConnectionManager = connectionManager }.ExecuteNonQuery();
        public static object ExecuteScalar(IConnectionManager connectionManager, string xmla) => new XmlaTask(xmla) { ConnectionManager = connectionManager }.ExecuteScalar();
        public static Nullable<T> ExecuteScalar<T>(IConnectionManager connectionManager, string xmla) where T : struct => new XmlaTask(xmla) { ConnectionManager = connectionManager }.ExecuteScalar<T>();
        public static bool ExecuteScalarAsBool(IConnectionManager connectionManager, string xmla) => new XmlaTask(xmla) { ConnectionManager = connectionManager }.ExecuteScalarAsBool();
        public static void ExecuteReader(IConnectionManager connectionManager, string xmla, params Action<object>[] actions) => new XmlaTask(xmla, actions) { ConnectionManager = connectionManager }.ExecuteReader();
        public static void ExecuteReader(IConnectionManager connectionManager, string xmla, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) =>
            new XmlaTask(xmla, beforeRowReadAction, afterRowReadAction, actions) { ConnectionManager = connectionManager }.ExecuteReader();
    }
}
