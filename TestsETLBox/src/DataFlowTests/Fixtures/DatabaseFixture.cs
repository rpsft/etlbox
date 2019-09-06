using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [CollectionDefinition("DataFlow")]
    public class DatalFlowCollectionClass : ICollectionFixture<DatabaseFixture> { }
    public class DatabaseFixture
    {
        public DatabaseFixture()
        {
            DatabaseHelper.RecreateDatabase(Config.SqlConnectionString("DataFlow").DBName
                , Config.SqlConnectionString("DataFlow"));
        }
    }

}
