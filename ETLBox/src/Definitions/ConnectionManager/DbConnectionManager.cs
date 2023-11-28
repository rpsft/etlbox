using System.Diagnostics;

namespace ALE.ETLBox.ConnectionManager
{
    [PublicAPI]
    [DebuggerDisplay("{ConnectionManagerType}:{ConnectionString}")]
    public abstract class DbConnectionManager<TConnection> : IConnectionManager
        where TConnection : class, IDbConnection, new()
    {
        public abstract ConnectionManagerType ConnectionManagerType { get; }

        public int MaxLoginAttempts { get; set; } = 3;
        public bool LeaveOpen
        {
            get => _leaveOpen || IsInBulkInsert || Transaction != null;
            set => _leaveOpen = value;
        }

        public IDbConnectionString ConnectionString { get; set; }

        protected TConnection DbConnection { get; private set; }
        public ConnectionState? State => DbConnection?.State;
        public IDbTransaction Transaction { get; set; }
        public bool IsInBulkInsert { get; set; }
        private bool _leaveOpen;

        public abstract string QB { get; }
        public abstract string QE { get; }
        public abstract CultureInfo ConnectionCulture { get; }
        public virtual bool SupportDatabases { get; } = true;
        public virtual bool SupportProcedures { get; } = true;
        public virtual bool SupportSchemas { get; } = true;
        public virtual bool SupportComputedColumns { get; } = true;

        protected DbConnectionManager() { }

        protected DbConnectionManager(IDbConnectionString connectionString)
            : this()
        {
            ConnectionString = connectionString;
        }

        public void Open()
        {
            if (LeaveOpen)
            {
                DbConnection ??= new TConnection { ConnectionString = ConnectionString.Value };
            }
            else
            {
                DbConnection?.Close();
                DbConnection = new TConnection { ConnectionString = ConnectionString.Value };
            }
            if (DbConnection.State != ConnectionState.Open)
            {
                TryOpenConnectionXTimes();
            }
        }

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

        public IDbCommand CreateCommand(
            string commandText,
            IEnumerable<QueryParameter> parameterList
        )
        {
            var cmd = DbConnection.CreateCommand();
            cmd.CommandTimeout = 0;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = commandText;
            if (parameterList != null)
            {
                foreach (QueryParameter par in parameterList)
                {
                    var newPar = cmd.CreateParameter();
                    MapQueryParameterToCommandParameter(par, newPar);
                    cmd.Parameters.Add(newPar);
                }
            }
            if (Transaction?.Connection is { State: ConnectionState.Open })
                cmd.Transaction = Transaction;
            return cmd;
        }

        /// <summary>
        /// Map QueryParameter to Command parameter
        /// </summary>
        /// <returns></returns>
        protected virtual void MapQueryParameterToCommandParameter(
            QueryParameter source,
            IDbDataParameter destination
        )
        {
            destination.ParameterName = source.Name;
            destination.DbType = source.DBType;
            destination.Value = source.Value;
        }

        public int ExecuteNonQuery(string command, IEnumerable<QueryParameter> parameterList = null)
        {
            IDbCommand cmd = CreateCommand(command, parameterList);
            return cmd.ExecuteNonQuery();
        }

        public object ExecuteScalar(
            string command,
            IEnumerable<QueryParameter> parameterList = null
        )
        {
            IDbCommand cmd = CreateCommand(command, parameterList);
            return cmd.ExecuteScalar();
        }

        public IDataReader ExecuteReader(
            string command,
            IEnumerable<QueryParameter> parameterList = null
        )
        {
            IDbCommand cmd = CreateCommand(command, parameterList);
            return cmd.ExecuteReader();
        }

        public IConnectionManager CloneIfAllowed()
        {
            return LeaveOpen ? this : Clone();
        }

        public void BeginTransaction(IsolationLevel isolationLevel)
        {
            Open();
            Transaction = DbConnection?.BeginTransaction(isolationLevel);
        }

        public void BeginTransaction() => BeginTransaction(IsolationLevel.Unspecified);

        public void CommitTransaction()
        {
            Transaction?.Commit();
            CloseTransaction();
        }

        public void RollbackTransaction()
        {
            Transaction?.Rollback();
            CloseTransaction();
        }

        public void CloseTransaction()
        {
            Transaction.Dispose();
            Transaction = null;
            CloseIfAllowed();
        }

        public abstract void PrepareBulkInsert(string tableName);
        public abstract void CleanUpBulkInsert(string tableName);

        public abstract void BulkInsert(ITableData data, string tableName);
        public abstract void BeforeBulkInsert(string tableName);
        public abstract void AfterBulkInsert(string tableName);

        #region IDisposable Support
        private bool _disposedValue; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (_disposedValue)
            {
                return;
            }

            if (disposing)
            {
                Transaction?.Dispose();
                Transaction = null;
                DbConnection?.Close();
                DbConnection = null;
            }
            _disposedValue = true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void CloseIfAllowed()
        {
            if (!LeaveOpen)
                Dispose();
        }

        public void Close()
        {
            Dispose();
        }

        public abstract IConnectionManager Clone();
        #endregion
    }
}
