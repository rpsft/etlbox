using ETLBox.Connection;
using System;
using System.Collections.Generic;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Executes any sql on the database. Use ExecuteNonQuery for SQL statements returning no data,
    /// ExecuteScalar for statements that return only one row and one column or
    /// ExecuteReader for SQL that returns multiple rows or columns
    /// </summary>
    /// <example>
    /// <code>
    /// SqlTask.ExecuteNonQuery("Description","insert into demo.table1 select * from demo.table2");
    /// </code>
    /// </example>
    public class SqlTask : DbTask
    {
        /// <inheritdoc/>
        public override string TaskName { get; set; } = "Run some sql";

        /// <summary>
        /// Executes a non query sql
        /// </summary>
        public void Execute() => ExecuteNonQuery();

        public SqlTask() : base()
        { }

        /// <param name="sql">Sets the <see cref="DbTask.Sql"/></param>
        public SqlTask(string sql) : base(sql) { }

        internal SqlTask(ControlFlowTask callingTask, string sql) : base(callingTask, sql) { }

        public SqlTask(string name, string sql) : base(name, sql) { }

        public SqlTask(string sql, IEnumerable<QueryParameter> parameter) : base(sql)
        {
            Parameter = parameter;
        }

        public SqlTask(string sql, params Action<object>[] actions) : base(sql, actions) { }

        public SqlTask(string sql, IEnumerable<QueryParameter> parameter, params Action<object>[] actions) : base(sql, actions)
        {
            Parameter = parameter;
        }

        public SqlTask(string sql, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions)
            : base(sql, beforeRowReadAction, afterRowReadAction, actions) { }

        public SqlTask(string sql, IEnumerable<QueryParameter> parameter, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) : base(sql, beforeRowReadAction, afterRowReadAction, actions)
        {
            Parameter = parameter;
        }

        /* Static methods for convenience */
        public static int ExecuteNonQuery(string sql) => new SqlTask(sql).ExecuteNonQuery();
        public static int ExecuteNonQuery(string name, string sql) => new SqlTask(name, sql).ExecuteNonQuery();
        public static int ExecuteNonQuery(IConnectionManager connectionManager, string sql) => new SqlTask(sql) { ConnectionManager = connectionManager }.ExecuteNonQuery();
        public static int ExecuteNonQuery(IConnectionManager connectionManager, string name, string sql) => new SqlTask(name, sql) { ConnectionManager = connectionManager }.ExecuteNonQuery();
        public static object ExecuteScalar(string sql) => new SqlTask(sql).ExecuteScalar();
        public static object ExecuteScalar(string name, string sql) => new SqlTask(name, sql).ExecuteScalar();
        public static object ExecuteScalar(IConnectionManager connectionManager, string sql) => new SqlTask(sql) { ConnectionManager = connectionManager }.ExecuteScalar();
        public static object ExecuteScalar(IConnectionManager connectionManager, string name, string sql) => new SqlTask(name, sql) { ConnectionManager = connectionManager }.ExecuteScalar();
        public static Nullable<T> ExecuteScalar<T>(string sql) where T : struct => new SqlTask(sql).ExecuteScalar<T>();
        public static Nullable<T> ExecuteScalar<T>(string name, string sql) where T : struct => new SqlTask(name, sql).ExecuteScalar<T>();
        public static Nullable<T> ExecuteScalar<T>(IConnectionManager connectionManager, string sql) where T : struct => new SqlTask(sql) { ConnectionManager = connectionManager }.ExecuteScalar<T>();
        public static Nullable<T> ExecuteScalar<T>(IConnectionManager connectionManager, string name, string sql) where T : struct => new SqlTask(name, sql) { ConnectionManager = connectionManager }.ExecuteScalar<T>();
        public static bool ExecuteScalarAsBool(string sql) => new SqlTask(sql).ExecuteScalarAsBool();
        public static bool ExecuteScalarAsBool(string name, string sql) => new SqlTask(name, sql).ExecuteScalarAsBool();
        public static bool ExecuteScalarAsBool(IConnectionManager connectionManager, string sql) => new SqlTask(sql) { ConnectionManager = connectionManager }.ExecuteScalarAsBool();
        public static bool ExecuteScalarAsBool(IConnectionManager connectionManager, string name, string sql) => new SqlTask(name, sql) { ConnectionManager = connectionManager }.ExecuteScalarAsBool();
        public static void ExecuteReaderSingleLine(string sql, params Action<object>[] actions) =>
           new SqlTask(sql, actions) { Limit = 1 }.ExecuteReader();
        public static void ExecuteReaderSingleLine(IConnectionManager connectionManager, string sql, params Action<object>[] actions)
            => new SqlTask(sql, actions) { ConnectionManager = connectionManager, Limit = 1 }.ExecuteReader();
        public static void ExecuteReader(string sql, params Action<object>[] actions) => new SqlTask(sql, actions).ExecuteReader();
        public static void ExecuteReader(string name, string sql, params Action<object>[] actions) => new SqlTask(sql, actions) { TaskName = name }.ExecuteReader();
        public static void ExecuteReader(IConnectionManager connectionManager, string sql, params Action<object>[] actions) => new SqlTask(sql, actions) { ConnectionManager = connectionManager }.ExecuteReader();
        public static void ExecuteReader(IConnectionManager connectionManager, string name, string sql, params Action<object>[] actions) => new SqlTask(sql, actions) { ConnectionManager = connectionManager, TaskName = name }.ExecuteReader();
        public static void ExecuteReader(string sql, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) =>
            new SqlTask(sql, beforeRowReadAction, afterRowReadAction, actions).ExecuteReader();
        public static void ExecuteReader(IConnectionManager connectionManager, string sql, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) =>
            new SqlTask(sql, beforeRowReadAction, afterRowReadAction, actions) { ConnectionManager = connectionManager }.ExecuteReader();
        public static int ExecuteNonQuery(string sql, IEnumerable<QueryParameter> parameterList) => new SqlTask(sql, parameterList).ExecuteNonQuery();
        public static int ExecuteNonQuery(string name, string sql, IEnumerable<QueryParameter> parameterList) => new SqlTask(sql, parameterList) { TaskName = name }.ExecuteNonQuery();
        public static int ExecuteNonQuery(IConnectionManager connectionManager, string sql, IEnumerable<QueryParameter> parameterList) => new SqlTask(sql, parameterList) { ConnectionManager = connectionManager }.ExecuteNonQuery();
        public static int ExecuteNonQuery(IConnectionManager connectionManager, string name, string sql, IEnumerable<QueryParameter> parameterList) => new SqlTask(sql, parameterList) { ConnectionManager = connectionManager, TaskName = name }.ExecuteNonQuery();
        public static object ExecuteScalar(string sql, IEnumerable<QueryParameter> parameterList) => new SqlTask(sql, parameterList).ExecuteScalar();
        public static object ExecuteScalar(string name, string sql, IEnumerable<QueryParameter> parameterList) => new SqlTask(sql, parameterList) { TaskName = name }.ExecuteScalar();
        public static object ExecuteScalar(IConnectionManager connectionManager, string sql, IEnumerable<QueryParameter> parameterList) => new SqlTask(sql, parameterList) { ConnectionManager = connectionManager }.ExecuteScalar();
        public static object ExecuteScalar(IConnectionManager connectionManager, string name, string sql, IEnumerable<QueryParameter> parameterList) => new SqlTask(sql, parameterList) { ConnectionManager = connectionManager, TaskName = name }.ExecuteScalar();
        public static Nullable<T> ExecuteScalar<T>(string sql, IEnumerable<QueryParameter> parameterList) where T : struct => new SqlTask(sql, parameterList).ExecuteScalar<T>();
        public static bool ExecuteScalarAsBool(string sql, IEnumerable<QueryParameter> parameterList) => new SqlTask(sql, parameterList).ExecuteScalarAsBool();
        public static void ExecuteReaderSingleLine(string sql, IEnumerable<QueryParameter> parameterList, params Action<object>[] actions)
            => new SqlTask(sql, parameterList, actions) { Limit = 1 }.ExecuteReader();
        public static void ExecuteReaderSingleLine(IConnectionManager connectionManager, string sql, IEnumerable<QueryParameter> parameterList, params Action<object>[] actions)
    => new SqlTask(sql, parameterList, actions) { ConnectionManager = connectionManager, Limit = 1 }.ExecuteReader();
        public static void ExecuteReader(string sql, IEnumerable<QueryParameter> parameterList, params Action<object>[] actions) => new SqlTask(sql, parameterList, actions).ExecuteReader();
        public static void ExecuteReader(IConnectionManager connectionManager, string sql, IEnumerable<QueryParameter> parameterList, params Action<object>[] actions) => new SqlTask(sql, parameterList, actions) { ConnectionManager = connectionManager }.ExecuteReader();
        public static void ExecuteReader(string sql, IEnumerable<QueryParameter> parameterList, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) =>
            new SqlTask(sql, parameterList, beforeRowReadAction, afterRowReadAction, actions).ExecuteReader();
        public static void BulkInsert(string name, ITableData data, string tableName) =>
            new SqlTask() { TaskName = name }.BulkInsert(data, tableName);
        public static void BulkInsert(IConnectionManager connectionManager, ITableData data, string tableName) =>
            new SqlTask() { ConnectionManager = connectionManager }.BulkInsert(data, tableName);
        public static void BulkInsert(IConnectionManager connectionManager, string name, ITableData data, string tableName) =>
            new SqlTask() { ConnectionManager = connectionManager, TaskName = name }.BulkInsert(data, tableName);

    }

}
