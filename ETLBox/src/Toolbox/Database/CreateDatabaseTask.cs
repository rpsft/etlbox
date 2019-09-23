using ALE.ETLBox.ConnectionManager;
using System;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Will create a database if the database doesn't exists.
    /// </summary>
    /// <example>
    /// <code>
    /// CreateDatabaseTask.Create("DemoDB");
    /// </code>
    /// </example>
    public class CreateDatabaseTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "CREATEDB";
        public override string TaskName => $"Create DB {DatabaseName}";
        public override void Execute()
        {
            if (ConnectionType == ConnectionManagerType.SQLite)
                throw new ETLBoxNotSupportedException("This task is not supported with SQLite!");
            bool doesExist = new IfDatabaseExistsTask(DatabaseName) { DisableLogging = true, ConnectionManager = ConnectionManager }.DoesExist;
            if (!doesExist)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        public void Create() => Execute();

        /* Public properties */
        public string DatabaseName { get; set; }
        public RecoveryModel RecoveryModel { get; set; } = RecoveryModel.Simple;
        public string Collation { get; set; }
        public string Sql
        {
            get
            {
//                if (ConnectionType == ConnectionManagerType.SqlServer)
//                {
//                    return
//        $@"
//IF (db_id('{DatabaseName}') IS NULL)
//BEGIN
//    USE [master]

//    CREATE DATABASE [{DatabaseName}] {CollationString}
//    {RecoveryString}
//    ALTER DATABASE [{DatabaseName}] SET AUTO_CREATE_STATISTICS ON
//    ALTER DATABASE [{DatabaseName}] SET AUTO_UPDATE_STATISTICS ON
//    ALTER DATABASE [{DatabaseName}] SET AUTO_UPDATE_STATISTICS_ASYNC OFF
//    ALTER DATABASE [{DatabaseName}] SET AUTO_CLOSE OFF
//    ALTER DATABASE [{DatabaseName}] SET AUTO_SHRINK OFF

//    --wait for database to enter 'ready' state
//    DECLARE @dbReady BIT = 0
//    WHILE (@dbReady = 0)
//    BEGIN
//    SELECT @dbReady = CASE WHEN DATABASEPROPERTYEX('{DatabaseName}', 'Collation') IS NULL THEN 0 ELSE 1 END                    
//    END
//END
//";
//                }
//                else if (ConnectionType == ConnectionManagerType.MySql)
//                {
//                    return $@"CREATE DATABASE IF NOT EXISTS {DatabaseName} {CollationString}";
//                }
//                else
//                {
                    return $@"CREATE DATABASE {QB}{DatabaseName}{QE}";
                //}
            }
        }

        /* Some constructors */
        public CreateDatabaseTask()
        {
        }

        public CreateDatabaseTask(string databaseName) : this()
        {
            DatabaseName = databaseName;
        }

        public CreateDatabaseTask(string databaseName, RecoveryModel recoveryModel) : this(databaseName)
        {
            RecoveryModel = recoveryModel;
        }

        public CreateDatabaseTask(string databaseName, RecoveryModel recoveryModel, string collation) : this(databaseName, recoveryModel)
        {
            Collation = collation;
        }

        /* Static methods for convenience */
        public static void Create(string databaseName) => new CreateDatabaseTask(databaseName).Execute();
        public static void Create(string databaseName, RecoveryModel recoveryModel) => new CreateDatabaseTask(databaseName, recoveryModel).Execute();
        public static void Create(string databaseName, RecoveryModel recoveryModel, string collation) => new CreateDatabaseTask(databaseName, recoveryModel, collation).Execute();
        public static void Create(IConnectionManager connectionManager, string databaseName)
            => new CreateDatabaseTask(databaseName) { ConnectionManager = connectionManager }.Execute();
        public static void Create(IConnectionManager connectionManager, string databaseName, RecoveryModel recoveryModel)
            => new CreateDatabaseTask(databaseName, recoveryModel) { ConnectionManager = connectionManager }.Execute();
        public static void Create(IConnectionManager connectionManager, string databaseName, RecoveryModel recoveryModel, string collation)
            => new CreateDatabaseTask(databaseName, recoveryModel, collation) { ConnectionManager = connectionManager }.Execute();

        /* Implementation & stuff */
        string RecoveryModelAsString
        {
            get
            {
                if (RecoveryModel == RecoveryModel.Simple)
                    return "SIMPLE";
                else if (RecoveryModel == RecoveryModel.BulkLogged)
                    return "BULK";
                else if (RecoveryModel == RecoveryModel.Full)
                    return "FULL";
                else return string.Empty;
            }
        }
        bool HasCollation => !String.IsNullOrWhiteSpace(Collation);
        string CollationString => HasCollation ? "COLLATE " + Collation : string.Empty;
        string RecoveryString => RecoveryModel != RecoveryModel.Default ?
            $"ALTER DATABASE [{DatabaseName}] SET RECOVERY {RecoveryModelAsString} WITH no_wait"
            : string.Empty;

    }

    public enum RecoveryModel
    {
        Default, Simple, BulkLogged, Full
    }

}
