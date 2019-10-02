using ALE.ETLBox.ConnectionManager;
using System;
using System.Data;
using System.Linq;

namespace ALE.ETLBox.ControlFlow.SqlServer {
    /// <summary>
    /// This task can exeucte any XMLA.
    /// </summary>
    /// <example>
    /// <code>
    /// XmlaTask.ExecuteNonQuery("Log description here","Xmla goes here...")
    /// </code>
    /// </example>
    public class XmlaTask : DbTask {
        public override string TaskName { get; set; } = "Run some xmla";
        public override void Execute() => ExecuteNonQuery();

        public XmlaTask() {
            Init();
        }

        public XmlaTask(string name) : base(name) {
            Init();
        }

        public XmlaTask(string name, FileConnectionManager fileConnection) : base(name, fileConnection) {
            Init();
        }

        public XmlaTask(ITask callingTask, string sql) : base(callingTask, sql) {
            Init();
        }

        public XmlaTask(string name, string sql) : base(name, sql) {
            Init();
        }

        public XmlaTask(string name, string sql, params Action<object>[] actions) : base(name, sql, actions) {
            Init();
        }

        public XmlaTask(string name, string sql, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) : base(name, sql, beforeRowReadAction, afterRowReadAction, actions) {
            Init();
        }

        private void Init() {
            DoXMLCommentStyle = true;
        }

        /* Static methods for convenience */
        public static int ExecuteNonQuery(string name, string sql) => new XmlaTask(name, sql).ExecuteNonQuery();
        public static int ExecuteNonQuery(string name, FileConnectionManager fileConnection) => new XmlaTask(name, fileConnection).ExecuteNonQuery();
        public static object ExecuteScalar(string name, string sql) => new XmlaTask(name, sql).ExecuteScalar();
        public static Nullable<T> ExecuteScalar<T>(string name, string sql) where T : struct => new XmlaTask(name, sql).ExecuteScalar<T>();
        public static bool ExecuteScalarAsBool(string name, string sql) => new XmlaTask(name, sql).ExecuteScalarAsBool();
        public static void ExecuteReaderSingleLine(string name, string sql, params Action<object>[] actions) =>
           new XmlaTask(name, sql, actions) { ReadTopX = 1 }.ExecuteReader();
        public static void ExecuteReader(string name, string sql, params Action<object>[] actions) => new XmlaTask(name, sql, actions).ExecuteReader();
        public static void ExecuteReader(string name, string sql, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) =>
            new XmlaTask(name, sql, beforeRowReadAction, afterRowReadAction, actions).ExecuteReader();
        public static void ExecuteReader(string name, FileConnectionManager fileConnection, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) =>
            new XmlaTask(name, fileConnection) { BeforeRowReadAction = beforeRowReadAction, AfterRowReadAction = afterRowReadAction, Actions = actions.ToList() }.ExecuteReader();
        public static int ExecuteNonQuery(IConnectionManager connectionManager, string name, string sql) => new XmlaTask(name, sql) { ConnectionManager = connectionManager }.ExecuteNonQuery();
        public static int ExecuteNonQuery(IConnectionManager connectionManager, string name, FileConnectionManager fileConnection) => new XmlaTask(name, fileConnection) { ConnectionManager = connectionManager }.ExecuteNonQuery();
        public static object ExecuteScalar(IConnectionManager connectionManager, string name, string sql) => new XmlaTask(name, sql) { ConnectionManager = connectionManager }.ExecuteScalar();
        public static Nullable<T> ExecuteScalar<T>(IConnectionManager connectionManager, string name, string sql) where T : struct => new XmlaTask(name, sql) { ConnectionManager = connectionManager }.ExecuteScalar<T>();
        public static bool ExecuteScalarAsBool(IConnectionManager connectionManager, string name, string sql) => new XmlaTask(name, sql) { ConnectionManager = connectionManager }.ExecuteScalarAsBool();
        public static void ExecuteReader(IConnectionManager connectionManager, string name, string sql, params Action<object>[] actions) => new XmlaTask(name, sql, actions) { ConnectionManager = connectionManager }.ExecuteReader();
        public static void ExecuteReader(IConnectionManager connectionManager, string name, string sql, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) =>
            new XmlaTask(name, sql, beforeRowReadAction, afterRowReadAction, actions) { ConnectionManager = connectionManager }.ExecuteReader();

        //public static void BulkInsert(string name, ITableData data, string tableName) =>
        //    new XmlaTask(name).BulkInsert(data, tableName);
    }
}
