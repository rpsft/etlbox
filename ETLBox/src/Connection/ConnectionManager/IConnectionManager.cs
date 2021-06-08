using ETLBox.ControlFlow;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Data;

namespace ETLBox.Connection
{
    /// <summary>
    /// Common properties and methods for all database connection managers
    /// </summary>
    public interface IConnectionManager : IDisposable
    {
        /// <summary>
        /// The database type for the connection manager.
        /// </summary>
        ConnectionManagerType ConnectionManagerType { get; }

        /// <summary>
        /// Number of attempts that the connection managers tries to connect before it decides that the database is not reachable.
        /// </summary>
        int MaxLoginAttempts { get; set; }

        /// <summary>
        /// By default, after every sql operation the underlying ADO.NET connection is closed and retured to the ADO.NET connection pool.
        /// (This is the recommended behavior)
        /// To keep the connection open and avoid having the connection returned to the pool, set this to true.
        /// A connnection will be left open when a bulk insert operation is executed or a transaction hase been openend and not yet commited or rolled back.
        /// </summary>
        bool LeaveOpen { get; set; }

        /// <summary>
        /// The connection string used to establish the connection with the database
        /// </summary>
        IDbConnectionString ConnectionString { get; set; }

        /// <summary>
        /// The state of the underlying ADO.NET connection
        /// </summary>
        ConnectionState? State { get; }

        /// <summary>
        /// The quotation begin character that is used in the database.
        /// E.g. SqlServer uses: '[' and Postgres: '"'
        /// </summary>        
        string QB { get; }

        /// <summary>
        /// The quotation end character that is used in the database.
        /// E.g. SqlServer uses: ']' and Postgres: '"'
        /// </summary>
        string QE { get; }

        /// <summary>
        /// The character that is used in front of parameter names in query to identify the parameter.
        /// All databases use the '@' character, except Oracle which uses ':'
        /// </summary>
        string PP { get; }

        /// <summary>
        /// Indicates if database server does support multiple databases.
        /// A database in ETLBox means a schema in MySql.
        /// </summary>
        bool SupportDatabases { get; }

        /// <summary>
        /// Indicates if the database supports procedures
        /// </summary>
        bool SupportProcedures { get; }

        /// <summary>
        /// Indicates if the database supports schemas
        /// In MySql, this is false because the schema here is a database in ETLBox.
        /// Use <see cref="SupportDatabases"/> instead
        /// </summary>
        bool SupportSchemas { get; }

        /// <summary>
        /// Indicates if the current connection mangager is used as a OleDb or Odbc Connection.
        /// </summary>
        bool IsOdbcOrOleDbConnection { get; }

        /// <summary>
        /// returns the maximum amount of parameters that ca be passed into a
        /// sql query. 
        /// </summary>
        int MaxParameterAmount { get; }

        /// <summary>
        /// Executes a query against the database that doesn't return any data.
        /// </summary>
        /// <param name="command">The sql command</param>
        /// <param name="parameterList">The optional list of parameters</param>
        /// <returns>Number of affected rows.</returns>
        int ExecuteNonQuery(string command, IEnumerable<QueryParameter> parameterList = null);

        /// <summary>
        /// Executes a query against the database that does return only one row in one column.
        /// </summary>
        /// <param name="command">The sql command</param>
        /// <param name="parameterList">The optional list of parameters</param>
        /// <returns>The result</returns>
        object ExecuteScalar(string command, IEnumerable<QueryParameter> parameterList = null);

        /// <summary>
        /// Executes a query against the database that does return multiple rows in multiple columns
        /// </summary>
        /// <param name="command">The sql command</param>
        /// <param name="parameterList">The optional list of parameters</param>
        /// <returns>A data reader to iterate through the result set</returns>
        IDataReader ExecuteReader(string command, IEnumerable<QueryParameter> parameterList = null);

        /// <summary>
        /// Will start a transaction with the given isolation level (if supported by the target database) 
        /// This will leave the underlying ADO.NET connection open until the transaction is committed or rolled back.
        /// </summary>
        /// <param name="isolationLevel">The isolation level for the transaction</param>
        void BeginTransaction(IsolationLevel isolationLevel);

        /// <summary>
        /// Will start a transaction with the default isolation level.
        /// This will leave the underlying ADO.NET connection open until the transaction is committed or rolled back.
        /// </summary>
        void BeginTransaction();

        /// <summary>
        /// Commits the current tranasction.
        /// </summary>
        void CommitTransaction();

        /// <summary>
        /// Rolls the current transaction back.
        /// </summary>
        void RollbackTransaction();

        /// <summary>
        /// Try to create a clone of the current connection - only possible if <see cref="LeaveOpen"/> is false.
        /// </summary>
        /// <returns>The connection that was either cloned or the current connection</returns>
        IConnectionManager CloneIfAllowed();

        /// <summary>
        /// Cretes a clone of the current connection manager
        /// </summary>
        /// <returns>A instance copy of the current connection manager</returns>
        IConnectionManager Clone();

        /// <summary>
        /// Opens the connection to the database. Normally you don't have to do this on your own,
        /// as all tasks and components will call this method implictly if the connection is closed.
        /// If the connection is already open, nothing is done.
        /// </summary>
        void Open();

        /// <summary>
        /// Always closes the connection
        /// </summary>
        void Close();

        /// <summary>
        /// Closes the connection if leave open is false and no transaction or bulk insert is in progress.
        /// </summary>
        void CloseIfAllowed();

        /// <summary>
        /// Indicates if the current connection is currently used in a bulk insert operation (e.g. performed by a DbDestination)
        /// </summary>
        bool IsInBulkInsert { get; set; }

        /// <summary>
        /// Performs preparations needed to improved 
        /// performance of a bulk insert operation
        /// </summary>
        /// <param name="tableName">Destination table name</param>
        void PrepareBulkInsert(string tableName);

        /// <summary>
        /// Performs a bulk insert
        /// </summary>
        /// <param name="data">Batch of data</param>        
        void BulkInsert(ITableData data);

        /// <summary>
        /// Called after the whole bulk insert operation 
        /// to change back settings made to improve bulk insert performance
        /// </summary>
        /// <param name="tableName">Destination table name</param>
        void CleanUpBulkInsert(string tableName);

        /// <summary>
        /// Performs a bulk delete
        /// </summary>
        /// <param name="data">Batch of data</param>        
        void BulkDelete(ITableData data);

        /// <summary>
        /// Performs a bulk update
        /// </summary>
        /// <param name="data">Batch of data</param>
        /// <param name="setColumnNames">The column names used in the set part of the update statement</param>
        /// <param name="joinColumnNames">The column names to join for the update</param>
        void BulkUpdate(ITableData data, ICollection<string> setColumnNames, ICollection<string> joinColumnNames);

        /// <summary>
        /// Performs a bulk select
        /// </summary>
        /// <param name="data">Batch of data needed for the where condition</param>
        /// <param name="selectColumnNames">Column names included in the select</param>
        /// <param name="beforeRowReadAction">Action invoked before any data is read</param>
        /// <param name="afterRowReadAction">Action invoked after all data is read</param>
        /// <param name="actions">Pass an action for each column</param>
        void BulkSelect(ITableData data, ICollection<string> selectColumnNames
            , Action beforeRowReadAction, Action afterRowReadAction
            , params Action<object>[] actions);
    }

    public interface IConnectionManagerDbObjects
    {
        /// <summary>
        /// Describe how the table meta data can be read from the database
        /// </summary>
        /// <param name="TN">The formatted table name</param>
        /// <returns>The definition of the table, containing column names, types, etc. </returns>
        TableDefinition ReadTableDefinition(ObjectNameDescriptor TN); //Access only


        /// <summary>
        /// Describes how the connection manager can check if a table or view exists
        /// </summary>
        /// <param name="objectName">The formatted table or view name</param>
        /// <returns>True if the table or view exists</returns>
        bool CheckIfTableOrViewExists(string objectName); //Access only

    }

    public interface IConnectionManager<TConnection, TTransaction> : IConnectionManager
         where TConnection : class, IDbConnection, new()
        where TTransaction : class, IDbTransaction
    {
        /// <summary>
        /// The underlying ADO.NET connection
        /// </summary>
        TConnection DbConnection { get; }

        /// <summary>
        /// The current transaction. Use <see cref="IConnectionManager.BeginTransaction()"/> to start a transaction,
        /// and <see cref="IConnectionManager.CommitTransaction()"/> or <see cref="IConnectionManager.RollbackTransaction()"/> to commit or rollback.
        /// </summary>
        TTransaction Transaction { get; }
    }
}
