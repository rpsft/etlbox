﻿using System.Data;
using System.Globalization;

namespace ALE.ETLBox.ConnectionManager
{
    [PublicAPI]
    public interface IConnectionManager : IDisposable
    {
        ConnectionManagerType ConnectionManagerType { get; }
        IDbConnectionString ConnectionString { get; set; }
        void Open();
        void Close();
        void CloseIfAllowed();
        ConnectionState? State { get; }
        int MaxLoginAttempts { get; set; }
        IDbCommand CreateCommand(string commandText, IEnumerable<QueryParameter> parameterList);
        int ExecuteNonQuery(string command, IEnumerable<QueryParameter> parameterList = null);
        object ExecuteScalar(string command, IEnumerable<QueryParameter> parameterList = null);
        IDataReader ExecuteReader(string command, IEnumerable<QueryParameter> parameterList = null);
        IDbTransaction Transaction { get; set; }
        void BulkInsert(ITableData data, string tableName);
        void BeforeBulkInsert(string tableName);
        void AfterBulkInsert(string tableName);
        IConnectionManager Clone();
        IConnectionManager CloneIfAllowed();
        bool LeaveOpen { get; set; }
        bool IsInBulkInsert { get; set; }
        void PrepareBulkInsert(string tableName);
        void CleanUpBulkInsert(string tableName);
        void BeginTransaction(IsolationLevel isolationLevel);
        void BeginTransaction();
        void CommitTransaction();
        void RollbackTransaction();
        void CloseTransaction();
        string QB { get; }
        string QE { get; }
        bool SupportDatabases { get; }
        bool SupportProcedures { get; }
        bool SupportSchemas { get; }
        bool SupportComputedColumns { get; }
        CultureInfo ConnectionCulture { get; }
    }
}
