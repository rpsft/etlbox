using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Text;

namespace ALE.ETLBox.Helper
{
    public class DatabaseHelper
    {
        public static void RecreateSqlDatabase(string section)
        {
            var connManagerMaster = new SqlConnectionManager(
                            Config.SqlConnection.ConnectionString(section).GetMasterConnection()
                            );
            var dbName = Config.SqlConnection.ConnectionString(section).DBName;
            new DropDatabaseTask(dbName)
            {
                DisableLogging = true,
                ConnectionManager = connManagerMaster
            }.Drop();

            new CreateDatabaseTask(dbName)
            {
                DisableLogging = true,
                ConnectionManager = connManagerMaster
            }.Execute();
        }

        public static void RecreateMySqlDatabase(string section)
        {
            var connManagerMaster = new MySqlConnectionManager(
                            Config.MySqlConnection.ConnectionString(section).GetMasterConnection()
                            );
            var dbName = Config.MySqlConnection.ConnectionString(section).DBName;
            new DropDatabaseTask(dbName)
            {
                DisableLogging = true,
                ConnectionManager = connManagerMaster
            }.Drop();

            new CreateDatabaseTask(dbName)
            {
                DisableLogging = true,
                ConnectionManager = connManagerMaster
            }.Execute();
        }
    }
}
