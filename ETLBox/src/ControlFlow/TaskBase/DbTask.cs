using ETLBox.Connection;
using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace ETLBox.ControlFlow
{
    public abstract class DbTask : ControlFlowTask
    {

        /* Public Properties */
        public string Sql { get; set; }
        public List<Action<object>> Actions { get; set; }
        public Action BeforeRowReadAction { get; set; }
        public Action AfterRowReadAction { get; set; }
        public int Limit { get; set; } = int.MaxValue;
        public int? RowsAffected { get; private set; }
        public IEnumerable<QueryParameter> Parameter { get; set; }

        public DbTask() {

        }

        public DbTask(string sql) : this() {
            this.Sql = sql;
        }

        public DbTask(string name, string sql) : this(sql) {
            this.TaskName = name;
        }

        public DbTask(ControlFlowTask callingTask, string sql) : this(sql) {
            CopyLogTaskProperties(callingTask);
            this.ConnectionManager = callingTask.ConnectionManager;
        }

        public DbTask(string sql, params Action<object>[] actions) : this(sql) {
            Actions = actions.ToList();
        }


        public DbTask(string sql, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) : this(sql, actions) {
            BeforeRowReadAction = beforeRowReadAction;
            AfterRowReadAction = afterRowReadAction;
            Actions = actions.ToList();
        }

        public int ExecuteNonQuery() {
            LogErrorsAndClose(conn => {
                LogInfo("{action}: Executing sql.", "START");
                LogTrace("{sql}", Sql);
                RowsAffected = conn.ExecuteNonQuery(Sql, Parameter);
                LogInfo("{action}: Sql execution completed - affected records {rowsAffected}", "END", RowsAffected);
            });
            return RowsAffected ?? 0;
        }

        public object ExecuteScalar() {
            object result = null;
            LogErrorsAndClose(conn => {
                LogInfo("{action}: Executing sql.", "START");
                LogTrace("{sql}", Sql);
                result = conn.ExecuteScalar(Sql, Parameter);
                LogInfo("{action}: Sql execution completed.", "END");
            });
            return result;
        }

        public Nullable<T> ExecuteScalar<T>() where T : struct {
            object result = ExecuteScalar();
            if (result == null || result == DBNull.Value)
                return null;
            else
                return (T)(Convert.ChangeType(result, typeof(T)));
        }


        public bool ExecuteScalarAsBool() {
            object result = ExecuteScalar();
            return ObjectToBool(result);
        }

        static bool ObjectToBool(object result) {
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

        public void ExecuteReader() {
            LogErrorsAndClose(conn => {
                try {
                    using (IDataReader reader = conn.ExecuteReader(Sql, Parameter) as IDataReader) {
                        LogInfo("{action}: Executing sql.", "START");
                        LogTrace("{sql}", Sql);
                        for (int rowNr = 0; rowNr < Limit; rowNr++) {
                            if (reader.Read()) {
                                BeforeRowReadAction?.Invoke();
                                for (int i = 0; i < Actions?.Count; i++) {
                                    if (!reader.IsDBNull(i)) {
                                        Actions?[i]?.Invoke(reader.GetValue(i));
                                    } else {
                                        Actions?[i]?.Invoke(null);
                                    }
                                }
                                AfterRowReadAction?.Invoke();
                            } else {
                                break;
                            }
                        }
                        LogInfo("{action}: Sql execution completed.", "END");
                    }
                } catch (IndexOutOfRangeException ir) {
                    int count = Actions?.Count ?? 0;
                    var ex = new ETLBoxException($"Error when executing a read action - all columns in the source sql statement were already read by the previous actions. Please define only as many actions as columns returned in your sql statement. You defined {count} actions, which exceeded the number of returned columns in the sql. See Data[\"Sql\"] for the used sql statement.", ir);
                    ex.Data.Add("Sql", Sql);
                    throw ex;
                }
            });
        }

        public void BulkInsert(ITableData data) {
            if (data.ColumnMapping?.Count == 0) throw new ETLBoxException("A mapping between the columns in your destination table " +
                "and the properties in your source data could not be automatically retrieved. There were no matching entries found.");
            LogErrorsAndClose(conn => {
                LogDebug("{action}: Sql bulk insert operation.", "START");
                conn.BulkInsert(data);
                RowsAffected = data.RecordsAffected;
                LogDebug("{action}: Sql bulk insert operation completed - affected records {rowsAffected}", "END", RowsAffected);
            });
        }

        public void BulkDelete(ITableData data) {
            if (data.ColumnMapping?.Count == 0) throw new ETLBoxException("A mapping between the columns in your destination table " +
                "and the properties in your source data could not be automatically retrieved. There were no matching entries found.");
            LogErrorsAndClose(conn => {
                LogDebug("{action}: Sql bulk delete operation.", "START");
                conn.BulkDelete(data);
                RowsAffected = data.RecordsAffected;
                LogDebug("{action}: Sql bulk delete operation completed - affected records {rowsAffected}", "END", RowsAffected);
            });
        }

        public void BulkUpdate(ITableData data, ICollection<string> setColumnNames, ICollection<string> joinColumnNames) {
            if (data.ColumnMapping?.Count == 0) throw new ETLBoxException("A mapping between the columns in your destination table " +
                "and the properties in your source data could not be automatically retrieved. There were no matching entries found.");
            LogErrorsAndClose(conn => {
                LogDebug("{action}: Sql bulk update operation.", "START");
                conn.BulkUpdate(data, setColumnNames, joinColumnNames);
                RowsAffected = data.RecordsAffected;
                LogDebug("{action}: Sql bulk update operation completed - affected records {rowsAffected}", "END", RowsAffected);
            });
        }

        public void LogErrorsAndClose(Action<IConnectionManager> a) {
            IConnectionManager conn = DbConnectionManager.CloneIfAllowed();
            try {
                conn.Open();
                a.Invoke(conn);
            } catch (Exception e) {
                LogError(e.Message);
                throw;
            } finally {
                conn.CloseIfAllowed();
            }
        }
    }
}
