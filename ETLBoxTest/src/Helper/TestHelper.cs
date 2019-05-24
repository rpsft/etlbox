using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;

namespace ALE.ETLBoxTest
{
    public static class TestHelper
    {
        internal static string RandomString(int length)
        {
            var random = new Random();
            const string pool = "abcdefghijklmnopqrstuvwxyz0123456789";
            var chars = Enumerable.Range(0, length)
                .Select(x => pool[random.Next(0, pool.Length)]);
            return new string(chars.ToArray());
        }

        internal static void RecreateDatabase(TestContext testContext)
        {
            string dbName = testContext.Properties["dbName"].ToString();
            string connectionString = testContext.Properties["connectionString"].ToString();
            RecreateDatabase(dbName, connectionString);
        }

        internal static void RecreateDatabase(string dbName, string connectionString)
        {
            var cs = new ConnectionString(connectionString);
            var masterConnection = new SqlConnectionManager(cs.GetMasterConnection());
            ControlFlow.SetLoggingDatabase(masterConnection);
            new DropDatabaseTask(dbName) { DisableLogging = true, ConnectionManager = masterConnection }.Execute();
            new CreateDatabaseTask(dbName, RecoveryModel.Simple, "Latin1_General_CS_AS") { DisableLogging = true, ConnectionManager = masterConnection }.Execute();
            ControlFlow.SetLoggingDatabase(new SqlConnectionManager(cs));
        }

        internal static string CreateCubeXMLA(string dbName)
        {
            return $@"<Alter AllowCreate=""true"" ObjectExpansion=""ExpandFull"" xmlns=""http://schemas.microsoft.com/analysisservices/2003/engine"">
  <Object>
    <DatabaseID>Cube</DatabaseID>
  </Object>
  <ObjectDefinition>
    <Database xmlns:xsd=""http://www.w3.org/2001/XMLSchema"" xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:ddl2=""http://schemas.microsoft.com/analysisservices/2003/engine/2"" xmlns:ddl2_2=""http://schemas.microsoft.com/analysisservices/2003/engine/2/2"" xmlns:ddl100_100=""http://schemas.microsoft.com/analysisservices/2008/engine/100/100"" xmlns:ddl200=""http://schemas.microsoft.com/analysisservices/2010/engine/200"" xmlns:ddl200_200=""http://schemas.microsoft.com/analysisservices/2010/engine/200/200"" xmlns:ddl300=""http://schemas.microsoft.com/analysisservices/2011/engine/300"" xmlns:ddl300_300=""http://schemas.microsoft.com/analysisservices/2011/engine/300/300"" xmlns:ddl400=""http://schemas.microsoft.com/analysisservices/2012/engine/400"" xmlns:ddl400_400=""http://schemas.microsoft.com/analysisservices/2012/engine/400/400"" xmlns:ddl500=""http://schemas.microsoft.com/analysisservices/2013/engine/500"" xmlns:ddl500_500=""http://schemas.microsoft.com/analysisservices/2013/engine/500/500"">
      <ID>{dbName}</ID>
      <Name>{dbName}</Name>
      <Description />
      <DataSourceImpersonationInfo>
        <ImpersonationMode>ImpersonateCurrentUser</ImpersonationMode>
      </DataSourceImpersonationInfo>
    </Database>
  </ObjectDefinition>
</Alter>";
        }

        internal static string DeleteCubeXMLA(string dbName)
        {
            return $@"<Delete xmlns=""http://schemas.microsoft.com/analysisservices/2003/engine"">
  <Object>
    <DatabaseID>{dbName}</DatabaseID>
  </Object>
</Delete>";
        }



    }
}
