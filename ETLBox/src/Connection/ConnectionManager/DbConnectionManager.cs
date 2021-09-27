using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace ETLBox.Connection
{
    /// <summary>
    /// The generic implementation on which all connection managers are based on
    /// </summary>
    /// <typeparam name="TConnection">The underlying ADO.NET connection</typeparam>
    /// <typeparam name="TTransaction">The transaction type used in the ADO.NET connector</typeparam>
    /// <typeparam name="TParameter">The parameter type used in the ADO.NET connector</typeparam>
    public abstract class DbConnectionManager<TConnection, TTransaction, TParameter> : IDisposable, IConnectionManager<TConnection, TTransaction>
        where TConnection : class, IDbConnection, new()
        where TTransaction : class, IDbTransaction
        where TParameter : class, IDbDataParameter, new()
    {
        /// <summary>
        /// The underlying ADO.NET connection.
        /// Only read from this object and it's properties - by default, connections are always
        /// acquired from the connection pool. There is no guarantee that
        /// the same connection will be used in ETLBox components.
        /// </summary>
        public TConnection DbConnection { get; protected set; }

        public DbConnectionManager() {
        }

        public DbConnectionManager(IDbConnectionString connectionString) : this() {
            this.ConnectionString = connectionString;
        }

        #region IConnectionManager interface

        /// <inheritdoc/>
        public abstract ConnectionManagerType ConnectionManagerType { get; protected set; }

        /// <inheritdoc/>
        public int MaxLoginAttempts { get; set; } = 3;

        /// <inheritdoc/>
        public bool LeaveOpen {
            get => _leaveOpen || IsInBulkInsert || Transaction != null;
            set => _leaveOpen = value;
        }
        private bool _leaveOpen;

        /// <inheritdoc/>
        public IDbConnectionString ConnectionString { get; set; }

        /// <inheritdoc/>
        public ConnectionState? State => DbConnection?.State;

        /// <inheritdoc/>
        public TTransaction Transaction { get; set; }

        /// <inheritdoc/>
        public bool IsInBulkInsert { get; set; }

        public int CommandTimeout { get; set; } = 0;

        /// <inheritdoc/>
        public abstract string QB { get; protected set; }
        /// <inheritdoc/>
        public abstract string QE { get; protected set; }
        /// <inheritdoc/>
        public virtual string PP { get; protected set; } = "@";

        /// <inheritdoc/>
        public virtual bool SupportDatabases { get; } = true;

        /// <inheritdoc/>
        public virtual bool SupportProcedures { get; } = true;

        /// <inheritdoc/>
        public virtual bool SupportSchemas { get; } = true;

        /// <inheritdoc/>
        public virtual bool IsOdbcOrOleDbConnection { get; } = false;
                
        /// <inheritdoc/>
        public virtual int MaxParameterAmount { get; } = int.MaxValue;

        public virtual string Compatibility { get; set; } = string.Empty;

        /// <summary>
        /// Changes the connection manager type for the generic connector, so that
        /// you can try to use it with not supported setups. 
        /// If you are looking for supported Odbc connection managers, try to use the specific
        /// connection managers (e.g. MySqlOdbcConnectionManager for MySql or 
        /// PostgresOdbcConnectionManager for Postgres)
        /// </summary>
        /// <param name="connectionManagerType">The new connection type for this connection manager.</param>
        /// <param name="QB">Quotation begin (e.g. "`" for MySql or "[" for SqlServer)</param>
        /// <param name="QE">Quotation end (e.g. "`" for MySql or "]" for SqlServer)</param>
        /// <param name="PP">Parameter placeholder ("@" for most databases)</param>
        public void OverrideConnectionSpecifics(ConnectionManagerType connectionManagerType,
            string QB, string QE, string PP = "@") {
            this.ConnectionManagerType = connectionManagerType;
            this.QB = QB;
            this.QE = QE;
            this.PP = PP;
        }

        internal IDbCommand CreateCommand(string commandText,
            IEnumerable<QueryParameter> queryParameterList = null,
            IEnumerable<TParameter> adonetParameterList = null
            ) {
            var cmd = DbConnection.CreateCommand();
            cmd.CommandTimeout = CommandTimeout;
            cmd.CommandText = commandText;
            if (queryParameterList != null) {
                foreach (QueryParameter par in queryParameterList) {
                    var newPar = cmd.CreateParameter();
                    if (!string.IsNullOrEmpty(par.Name)) newPar.ParameterName = par.Name;
                    if (par.DBType != null) newPar.DbType = par.DBType ?? DbType.String;
                    newPar.Value = par.Value;
                    if (par.DBSize > 0)
                        newPar.Size = par.DBSize;
                    cmd.Parameters.Add(newPar);
                }
            } else if (adonetParameterList != null) {
                foreach (TParameter parameter in adonetParameterList)
                    cmd.Parameters.Add(parameter);
            }
            if (Transaction?.Connection != null && Transaction.Connection.State == ConnectionState.Open)
                cmd.Transaction = Transaction;
            return cmd;
        }

        protected int BulkNonQuery(string commandText, IEnumerable<TParameter> parameterList) {
            IDbCommand cmd = CreateCommand(commandText, adonetParameterList: parameterList);
            return cmd.ExecuteNonQuery();
        }

        protected void BulkReader(string commandText,
            IEnumerable<TParameter> parameterList,
            Action beforeRowReadAction,
            Action afterRowReadAction,
            params Action<object>[] rowActions            
            ) {
            IDbCommand cmd = CreateCommand(commandText, adonetParameterList: parameterList);
            using (IDataReader reader = cmd.ExecuteReader() as IDataReader) {
                while (reader.Read()) {
                    beforeRowReadAction?.Invoke();

                    for (int i = 0; i < rowActions?.Length; i++) {
                        if (!reader.IsDBNull(i))
                            rowActions?[i]?.Invoke(reader.GetValue(i));
                        else
                            rowActions?[i]?.Invoke(null);
                    }
                    afterRowReadAction?.Invoke();
                }
            }
        }

        /// <inheritdoc/>
        public int ExecuteNonQuery(string commandText, IEnumerable<QueryParameter> parameterList = null) {
            IDbCommand cmd = CreateCommand(commandText, parameterList);
            int affected;
            try {
                affected = cmd.ExecuteNonQuery();
            } catch (Exception e) {
                AddSqlToDataDict(commandText, e);
                throw e;
            }
            return affected;

        }

        private void AddSqlToDataDict(string commandText, Exception e) {
            if (e.Data != null && !e.Data.Contains("Sql")) e.Data.Add("Sql", commandText);
            e.Source = "ETLBox note: See the Data dictionary for the sql command that caused the error. Provider source information: " + e.Source;
        }

        /// <inheritdoc/>
        public object ExecuteScalar(string commandText, IEnumerable<QueryParameter> parameterList = null) {
            IDbCommand cmd = CreateCommand(commandText, parameterList);
            object result;
            try {
                result = cmd.ExecuteScalar();
            } catch (Exception e) {
                AddSqlToDataDict(commandText, e);
                throw e;
            }
            return result;
        }

        /// <inheritdoc/>
        public IDataReader ExecuteReader(string commandText, IEnumerable<QueryParameter> parameterList = null) {
            IDbCommand cmd = CreateCommand(commandText, parameterList);
            IDataReader reader;
            try {
                reader = cmd.ExecuteReader();
            } catch (Exception e) {
                AddSqlToDataDict(commandText, e);
                throw e;
            }
            return reader;
        }


        /// <inheritdoc/>
        public void BeginTransaction(IsolationLevel isolationLevel) {
            Open();
            Transaction = DbConnection?.BeginTransaction(isolationLevel) as TTransaction;
        }

        /// <inheritdoc/>
        public void BeginTransaction() => BeginTransaction(IsolationLevel.Unspecified);

        /// <inheritdoc/>
        public void CommitTransaction() {
            Transaction?.Commit();
            CloseTransaction();
        }

        /// <inheritdoc/>
        public void RollbackTransaction() {
            Transaction?.Rollback();
            CloseTransaction();
        }

        private void CloseTransaction() {
            Transaction.Dispose();
            Transaction = null;
            CloseIfAllowed();
        }

        /// <inheritdoc/>
        public abstract void PrepareBulkInsert(string tableName);

        /// <inheritdoc/>
        public abstract void BulkInsert(ITableData data);


        /// <inheritdoc/>
        public abstract void CleanUpBulkInsert(string tableName);

        /// <inheritdoc/>
        public abstract void BulkDelete(ITableData data);

        /// <inheritdoc/>
        public abstract void BulkUpdate(ITableData data, ICollection<string> setColumnNames, ICollection<string> joinColumnNames);


        public virtual void BulkSelect(ITableData data, ICollection<string> selectColumnNames, Action beforeRowReadAction, Action afterRowReadAction, params Action<object>[] actions) {

        }

        /// <inheritdoc/>
        public IConnectionManager CloneIfAllowed() {
            if (LeaveOpen) return this;
            else return Clone();
        }

        /// <inheritdoc/>
        public abstract IConnectionManager Clone();

        /// <summary>
        /// Copeis the connection manager base attribnutes from the current 
        /// connection manager to the target connection manager. 
        /// </summary>
        /// <param name="original">Target of the copy operation</param>
        public void CopyBaseAttributes(DbConnectionManager<TConnection, TTransaction, TParameter> original) {
            this.CommandTimeout = original.CommandTimeout;
        }

        /// <inheritdoc/>
        public void Open() {
            if (LeaveOpen) {
                if (DbConnection == null)
                    CreateDbConnection();
            } else {
                DbConnection?.Close();
                CreateDbConnection();
            }
            if (DbConnection.State != ConnectionState.Open)
                TryOpenConnectionXTimes();
        }

        /// <summary>
        /// By default, a db connection is created with the given connection string value.
        /// Override this method if you want to pass additional properties to the specific Ado.NET db connection. 
        /// </summary>
        public virtual void CreateDbConnection() {
            DbConnection = new TConnection {
                ConnectionString = ConnectionString.Value
            };
        }

        /// <inheritdoc/>
        public void Close() {
            Dispose();
        }

        /// <inheritdoc/>
        public void CloseIfAllowed() {
            if (!LeaveOpen)
                Dispose();
        }

        #endregion

        private void TryOpenConnectionXTimes() {
            bool successfullyConnected = false;
            Exception lastException = null;
            for (int i = 1; i <= MaxLoginAttempts; i++) {
                try {
                    DbConnection.Open();
                    successfullyConnected = true;
                } catch (Exception e) {
                    successfullyConnected = false;
                    lastException = e;
                    Task.Delay(1000).Wait();
                }
                if (successfullyConnected) {
                    break;
                }
            }
            if (!successfullyConnected) {
                throw lastException ?? new Exception("Could not connect to database!");
            }
        }

        #region IDisposable Support

        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing) {
            if (!disposedValue) {
                if (disposing) {
                    Transaction?.Dispose();
                    Transaction = null;
                    DbConnection?.Close();
                    DbConnection = null;
                }
                disposedValue = true;
            }
        }

        /// <summary>
        /// Closes the connection - this will not automatically disconnect
        /// from the database server, it will only return the connection 
        /// to the ADO.NET connection pool for further reuse.
        /// </summary>
        public void Dispose() {
            Dispose(true);
        }

        #endregion
    }
}
