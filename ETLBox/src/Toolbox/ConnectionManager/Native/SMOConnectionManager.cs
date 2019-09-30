using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data;

namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// Connection manager for Sql Server Managed Objects (SMO) connection to a sql server.
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.CurrentDbConnection = new SMOConnectionManager(new ConnectionString("Data Source=.;"));
    /// SqlTask.ExecuteNonQuery("sql with go keyword", @"insert into demo.table1 (value) select '####'; go 2");
    /// </code>
    /// </example>
    public class SMOConnectionManager : IConnectionManager, IDisposable
    {
        public IDbConnectionString ConnectionString { get; set; }
        public bool IsConnectionOpen => SqlConnectionManager.DbConnection?.State == ConnectionState.Open;

        public SMOConnectionManager(ConnectionString connectionString)
        {
            ConnectionString = connectionString;
            SqlConnectionManager = new SqlConnectionManager(connectionString);
        }

        public SMOConnectionManager(string connectionString) : this (new ConnectionString(connectionString))
        { }


        internal Server Server { get; set; }
        internal ServerConnection Context => Server.ConnectionContext;
        internal SqlConnectionManager SqlConnectionManager { get; set; }
        internal ServerConnection OpenedContext
        {
            get
            {
                if (!IsConnectionOpen)
                    Open();
                return Context;
            }
        }

        public void Open()
        {
            SqlConnectionManager = new SqlConnectionManager((ConnectionString)ConnectionString);
            SqlConnectionManager.Open();
            Server = new Server(new ServerConnection(SqlConnectionManager.DbConnection));
            Context.StatementTimeout = 0;
        }

        public IDbCommand CreateCommand(string commandText, IEnumerable<QueryParameter> parameterList = null)
            => SqlConnectionManager.CreateCommand(commandText,parameterList);

        public int ExecuteNonQuery(string command, IEnumerable<QueryParameter> parameterList = null)
            =>  OpenedContext.ExecuteNonQuery(command);


        public object ExecuteScalar(string command, IEnumerable<QueryParameter> parameterList = null)
            => OpenedContext.ExecuteScalar(command);

        public IDataReader ExecuteReader(string command, IEnumerable<QueryParameter> parameterList = null)
            => OpenedContext.ExecuteReader(command);

        public void BulkInsert(ITableData data, string tableName)
            => SqlConnectionManager.BulkInsert(data, tableName);

        public void BeforeBulkInsert(string tableName) => SqlConnectionManager.BeforeBulkInsert(tableName);

        public void AfterBulkInsert(string tableName) => SqlConnectionManager.AfterBulkInsert(tableName);


        private bool disposedValue = false; // To detect redundant calls
        protected void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Server?.ConnectionContext?.Disconnect();
                    if (SqlConnectionManager != null)
                        SqlConnectionManager.Close();
                    SqlConnectionManager = null;
                    Server = null;
                }
                disposedValue = true;
            }
        }

        public void Dispose() => Dispose(true);
        public void Close() => Dispose();

        public IConnectionManager Clone()
        {
            SMOConnectionManager clone = new SMOConnectionManager((ConnectionString)ConnectionString) { };
            return clone;
        }


    }



}
