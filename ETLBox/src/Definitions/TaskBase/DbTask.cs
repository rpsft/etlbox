using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;

namespace ALE.ETLBox.ControlFlow
{
    public abstract class DbTask : GenericTask
    {

        /* Public Properties */
        public string Sql { get; set; }
        public List<Action<object>> Actions { get; set; }
        public Action BeforeRowReadAction { get; set; }
        public Action AfterRowReadAction { get; set; }
        public long Limit { get; set; } = long.MaxValue;
        public int? RowsAffected { get; private set; }
        public bool IsOdbcConnection => DbConnectionManager.GetType().IsSubclassOf(typeof(OdbcConnectionManager));
        internal virtual string NameAsComment => CommentStart + TaskName + CommentEnd + Environment.NewLine;
        private string CommentStart => DoXMLCommentStyle ? @"<!--" : "/*";
        private string CommentEnd => DoXMLCommentStyle ? @"-->" : "*/";
        public virtual bool DoXMLCommentStyle { get; set; }
        public string Command
        {
            get
            {
                if (HasSql)
                    return HasName && !IsOdbcConnection ? NameAsComment + Sql : Sql;
                else
                    throw new Exception("Empty command");
            }
        }
        public IEnumerable<QueryParameter> Parameter { get; set; }

        /* Internal/Private properties */
        internal bool DoSkipSql { get; private set; }
        bool HasSql => !(String.IsNullOrWhiteSpace(Sql));

        /* Some constructors */
        public DbTask()
        {

        }

        public DbTask(string name) : this()
        {
            this.TaskName = name;
        }

        public DbTask(string name, string sql) : this(name)
        {
            this.Sql = sql;
        }

        public DbTask(ITask callingTask, string sql)
        {
            this.Sql = sql;
            CopyTaskProperties(callingTask);
        }

        public DbTask(string name, string sql, params Action<object>[] actions) : this(name, sql)
        {
            Actions = actions.ToList();
        }


        public DbTask(string name, string sql, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) : this(name, sql)
        {
            BeforeRowReadAction = beforeRowReadAction;
            AfterRowReadAction = afterRowReadAction;
            Actions = actions.ToList();
        }


        /* Public methods */
        public int ExecuteNonQuery()
        {
            var conn = DbConnectionManager.Clone();
            try
            {
                conn.Open();
                if (!DisableLogging) LoggingStart();
                RowsAffected = DoSkipSql ? 0 : conn.ExecuteNonQuery(Command, Parameter);
                if (!DisableLogging) LoggingEnd(LogType.Rows);
            }
            finally
            {
                if (!conn.LeaveOpen)
                    conn.Close();
            }
            return RowsAffected ?? 0;
        }

        public object ExecuteScalar()
        {
            object result = null;
            var conn = DbConnectionManager.Clone();
            try
            {
                conn.Open();
                if (!DisableLogging) LoggingStart();
                result = conn.ExecuteScalar(Command, Parameter);
                if (!DisableLogging) LoggingEnd();
            }
            finally
            {
                if (!conn.LeaveOpen)
                    conn.Close();
            }
            return result;
        }

        public Nullable<T> ExecuteScalar<T>() where T : struct
        {
            object result = ExecuteScalar();
            if (result == null || result == DBNull.Value)
                return null;
            else
                return (T)(Convert.ChangeType(result, typeof(T)));
        }


        public bool ExecuteScalarAsBool()
        {
            object result = ExecuteScalar();
            return ObjectToBool(result);
        }

        static bool ObjectToBool(object result)
        {
            if (result == null) return false;
            int number = 0;
            int.TryParse(result.ToString(), out number);
            if (number > 0)
                return true;
            else if (result.ToString().Trim().ToLower() == "true")
                return true;
            else
                return false;
        }

        public void ExecuteReader()
        {
            var conn = DbConnectionManager.Clone();
            try
            {
                conn.Open();
                if (!DisableLogging) LoggingStart();
                IDataReader reader = conn.ExecuteReader(Command, Parameter) as IDataReader;
                for (int rowNr = 0; rowNr < Limit; rowNr++)
                {
                    if (reader.Read())
                    {
                        BeforeRowReadAction?.Invoke();
                        for (int i = 0; i < Actions?.Count; i++)
                        {
                            if (!reader.IsDBNull(i))
                            {
                                Actions?[i]?.Invoke(reader.GetValue(i));
                            }
                            else
                            {
                                Actions?[i]?.Invoke(null);
                            }
                        }
                        AfterRowReadAction?.Invoke();
                    }
                    else
                    {
                        break;
                    }
                }
                reader.Close();
                if (!DisableLogging) LoggingEnd();
            }
            finally
            {
                if (!conn.LeaveOpen)
                    conn.Close();
            }
        }


        public void BulkInsert(ITableData data, string tableName)
        {
            var conn = DbConnectionManager.Clone();
            try
            {
                conn.Open();
                if (!DisableLogging) LoggingStart(LogType.Bulk);
                conn.BeforeBulkInsert(tableName);
                conn.BulkInsert(data, tableName);
                conn.AfterBulkInsert(tableName);
                RowsAffected = data.RecordsAffected;
                if (!DisableLogging) LoggingEnd(LogType.Bulk);
            }
            finally
            {
                if (!conn.LeaveOpen)
                    conn.Close();
            }
        }


        /* Private implementation & stuff */
        enum LogType
        {
            None,
            Rows,
            Bulk
        }


        void LoggingStart(LogType logType = LogType.None)
        {
            NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.Id);
            if (logType == LogType.Bulk)
                NLogger.Debug($"SQL Bulk Insert", TaskType, "RUN", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.Id);
            else
                NLogger.Debug($"{Command}", TaskType, "RUN", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.Id);
        }

        void LoggingEnd(LogType logType = LogType.None)
        {
            NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.Id);
            if (logType == LogType.Rows)
                NLogger.Debug($"Rows affected: {RowsAffected ?? 0}", TaskType, "RUN", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.Id);
        }
    }
}
