using ETLBox.ControlFlow;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace ETLBox.Connection
{
    /// <summary>
    /// The generic implementation on which all connection managers are based on
    /// </summary>
    /// <typeparam name="Connection">The underlying ADO.NET connection</typeparam>
    public abstract class DbConnectionManager<Connection> : IDisposable, IConnectionManager
        where Connection : class, IDbConnection, new()
    {
        /// <summary>
        /// The underlying ADO.NET connection
        /// </summary>
        public Connection DbConnection { get; set; }

        public DbConnectionManager()
        {
        }

        public DbConnectionManager(IDbConnectionString connectionString) : this()
        {
            this.ConnectionString = connectionString;
        }

        #region IConnectionManager interface

        /// <inheritdoc/>
        public abstract ConnectionManagerType ConnectionManagerType { get; }

        /// <inheritdoc/>
        public int MaxLoginAttempts { get; set; } = 3;

        /// <inheritdoc/>
        public bool LeaveOpen
        {
            get => _leaveOpen || IsInBulkInsert || Transaction != null;
            set => _leaveOpen = value;
        }
        private bool _leaveOpen;

        /// <inheritdoc/>
        public IDbConnectionString ConnectionString { get; set; }

        /// <inheritdoc/>
        public ConnectionState? State => DbConnection?.State;

        /// <inheritdoc/>
        public IDbTransaction Transaction { get; set; }

        /// <inheritdoc/>
        public bool IsInBulkInsert { get; set; }

        /// <inheritdoc/>
        public abstract string QB { get; }
        /// <inheritdoc/>
        public abstract string QE { get; }
        /// <inheritdoc/>
        public virtual string PP { get; } = "@";

        /// <inheritdoc/>
        public virtual bool SupportDatabases { get; } = true;

        /// <inheritdoc/>
        public virtual bool SupportProcedures { get; } = true;

        /// <inheritdoc/>
        public virtual bool SupportSchemas { get; } = true;

        /// <inheritdoc/>
        public virtual bool SupportComputedColumns { get; } = true;

        /// <inheritdoc/>
        public virtual bool IsOdbcOrOleDbConnection { get; } = false;

        /// <inheritdoc/>
        public virtual TableDefinition ReadTableDefinition(ObjectNameDescriptor TN)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public virtual bool CheckIfTableOrViewExists(string objectName)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public IDbCommand CreateCommand(string commandText, IEnumerable<QueryParameter> parameterList = null)
        {
            var cmd = DbConnection.CreateCommand();
            cmd.CommandTimeout = 0;
            cmd.CommandText = commandText;
            if (parameterList != null)
            {
                foreach (QueryParameter par in parameterList)
                {
                    var newPar = cmd.CreateParameter();
                    newPar.ParameterName = par.Name;
                    newPar.DbType = par.DBType;
                    newPar.Value = par.Value;
                    cmd.Parameters.Add(newPar);
                }
            }
            if (Transaction?.Connection != null && Transaction.Connection.State == ConnectionState.Open)
                cmd.Transaction = Transaction;
            return cmd;
        }

        /// <inheritdoc/>
        public int ExecuteNonQuery(string commandText, IEnumerable<QueryParameter> parameterList = null)
        {
            IDbCommand cmd = CreateCommand(commandText, parameterList);
            return cmd.ExecuteNonQuery();
        }

        /// <inheritdoc/>
        public object ExecuteScalar(string commandText, IEnumerable<QueryParameter> parameterList = null)
        {
            IDbCommand cmd = CreateCommand(commandText, parameterList);
            return cmd.ExecuteScalar();
        }

        /// <inheritdoc/>
        public IDataReader ExecuteReader(string commandText, IEnumerable<QueryParameter> parameterList = null)
        {
            IDbCommand cmd = CreateCommand(commandText, parameterList);
            return cmd.ExecuteReader();

        }

        /// <inheritdoc/>
        public void BeginTransaction(IsolationLevel isolationLevel)
        {
            Open();
            Transaction = DbConnection?.BeginTransaction(isolationLevel);
        }

        /// <inheritdoc/>
        public void BeginTransaction() => BeginTransaction(IsolationLevel.Unspecified);

        /// <inheritdoc/>
        public void CommitTransaction()
        {
            Transaction?.Commit();
            CloseTransaction();
        }

        /// <inheritdoc/>
        public void RollbackTransaction()
        {
            Transaction?.Rollback();
            CloseTransaction();
        }

        private void CloseTransaction()
        {
            Transaction.Dispose();
            Transaction = null;
            CloseIfAllowed();
        }

        /// <inheritdoc/>
        public abstract void PrepareBulkInsert(string tableName);

        /// <inheritdoc/>
        public abstract void BeforeBulkInsert(string tableName);

        /// <inheritdoc/>
        public abstract void BulkInsert(ITableData data, string tableName);

        /// <inheritdoc/>
        public abstract void AfterBulkInsert(string tableName);

        /// <inheritdoc/>
        public abstract void CleanUpBulkInsert(string tableName);

        /// <inheritdoc/>
        public IConnectionManager CloneIfAllowed()
        {
            if (LeaveOpen) return this;
            else return Clone();
        }

        /// <inheritdoc/>
        public abstract IConnectionManager Clone();

        /// <inheritdoc/>
        public void Open()
        {
            if (LeaveOpen)
            {
                if (DbConnection == null)
                {
                    DbConnection = new Connection
                    {
                        ConnectionString = ConnectionString.Value
                    };
                }
            }
            else
            {
                DbConnection?.Close();
                DbConnection = new Connection
                {
                    ConnectionString = ConnectionString.Value
                };
            }
            if (DbConnection.State != ConnectionState.Open)
            {
                TryOpenConnectionXTimes();
            }
        }

        /// <inheritdoc/>
        public void Close()
        {
            Dispose();
        }

        /// <inheritdoc/>
        public void CloseIfAllowed()
        {
            if (!LeaveOpen)
                Dispose();
        }

        /// <inheritdoc/>
        public virtual void CheckLicenseOrThrow(int progressCount)
        {
            ;
        }

        #endregion

        private void TryOpenConnectionXTimes()
        {
            bool successfullyConnected = false;
            Exception lastException = null;
            for (int i = 1; i <= MaxLoginAttempts; i++)
            {
                try
                {
                    DbConnection.Open();
                    successfullyConnected = true;
                }
                catch (Exception e)
                {
                    successfullyConnected = false;
                    lastException = e;
                    Task.Delay(1000).Wait();
                }
                if (successfullyConnected)
                {
                    break;
                }
            }
            if (!successfullyConnected)
            {
                throw lastException ?? new Exception("Could not connect to database!");
            }
        }

        #region IDisposable Support

        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Transaction?.Dispose();
                    Transaction = null;
                    DbConnection?.Close();
                    DbConnection = null;
                }
                disposedValue = true;
            }
        }

        /// <summary>
        /// Alyways closes the connection
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        #endregion
    }
}
