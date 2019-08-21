using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace ALE.ETLBox.Helper
{
    public static class DatabaseHelper
    {
        static DatabaseHelper()
        {

        }

        public static void RecreateDatabase(string dbName, ConnectionString connectionString)
        {
            var connManagerMaster = new SqlConnectionManager(
                    connectionString.GetMasterConnection()
                    );

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
