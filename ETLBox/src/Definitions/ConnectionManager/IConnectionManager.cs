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
        /// The current transaction. Use <see cref="BeginTransaction"/> to start a transaction,
        /// and <see cref="CommitTransaction"/> or <see cref="RollbackTransaction"/> to commit or rollback.
        /// </summary>
        IDbTransaction Transaction { get; set; }

        /// <summary>
        /// Indicates if the current connection is currently used in a bulk insert operation (e.g. performed by a DbDestination)
        /// </summary>
        bool IsInBulkInsert { get; set; }

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
        /// Indicates if the database supports computed columns
        /// </summary>
        bool SupportComputedColumns { get; }
        /// <summary>
        /// Indicates if the current connection mangager is used as a OleDb or Odbc Connection.
        /// </summary>
        bool IsOdbcOrOleDbConnection { get; }

        /// <summary>
        /// Describe how the table meta data can be read from the database
        /// </summary>
        /// <param name="TN">The formatted table name</param>
        /// <returns>The definition of the table, containing column names, types, etc. </returns>
        TableDefinition ReadTableDefinition(ObjectNameDescriptor TN);

        /// <summary>
        /// Describes how the connection manager can check if a table or view exists
        /// </summary>
        /// <param name="objectName">The formatted table or view name</param>
        /// <returns>True if the table or view exists</returns>
        bool CheckIfTableOrViewExists(string objectName);

        /// <summary>
        /// Creates a underlying ADO.NET command.
        /// </summary>
        /// <param name="commandText">The command text</param>
        /// <param name="parameterList">An optional list of parameters for the command</param>
        /// <returns>The ADO.NET command</returns>
        IDbCommand CreateCommand(string commandText, IEnumerable<QueryParameter> parameterList);

        /// <summary>
        /// Executes a query against the database that doesn't return any data.
        /// </summary>
        /// <param name="commandText">The sql command</param>
        /// <param name="parameterList">The optional list of parameters</param>
        /// <returns>Number of affected rows.</returns>
        int ExecuteNonQuery(string command, IEnumerable<QueryParameter> parameterList = null);
        /// <summary>
        /// Executes a query against the database that does return only one row in one column.
        /// </summary>
        /// <param name="commandText">The sql command</param>
        /// <param name="parameterList">The optional list of parameters</param>
        /// <returns>The result</returns>
        object ExecuteScalar(string command, IEnumerable<QueryParameter> parameterList = null);
        /// <summary>
        /// Executes a query against the database that does return multiple rows in multiple columns
        /// </summary>
        /// <param name="commandText">The sql command</param>
        /// <param name="parameterList">The optional list of parameters</param>
        /// <returns>A data reader to iterate through the result set</returns>
        IDataReader ExecuteReader(string command, IEnumerable<QueryParameter> parameterList = null);

        /// <summary>
        /// Will start a transaction. This will leave the underlying ADO.NET connection open until the transaction is committed or rolled back.
        /// </summary>
        /// <param name="isolationLevel">The isolation level for the transaction</param>
        void BeginTransaction(IsolationLevel isolationLevel);

        /// <summary>
        /// Will start a transaction. This will leave the underlying ADO.NET connection open until the transaction is committed or rolled back.
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
        /// Called before the whole bulk insert operation (all batches)
        /// </summary>
        /// <param name="tableName">Destination table name</param>
        void PrepareBulkInsert(string tableName);
        /// <summary>
        /// Called before every bulk insert of a batch
        /// </summary>
        /// <param name="tableName">Destination table name</param>
        void BeforeBulkInsert(string tableName);
        /// <summary>
        /// Performs a bulk insert
        /// </summary>
        /// <param name="data">Batch of data</param>
        /// <param name="tableName">Destination table name</param>
        void BulkInsert(ITableData data, string tableName);
        /// <summary>
        /// Called after every bulk insert of a batch
        /// </summary>
        /// <param name="tableName">Destination table name</param>
        void AfterBulkInsert(string tableName);
        /// <summary>
        /// Called after the whole bulk insert operation (all batches)
        /// </summary>
        /// <param name="tableName">Destination table name</param>
        void CleanUpBulkInsert(string tableName);

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
        /// Checks for the current license after 10.000 rows. If no license found, this will throw an exception.
        /// </summary>
        /// <param name="progressCount">Current number of rows</param>
        void CheckLicenseOrThrow(int count);
    }
}
