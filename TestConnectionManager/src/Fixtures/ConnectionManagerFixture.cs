using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.Fixtures
{
    [CollectionDefinition("Connection Manager", DisableParallelization = true)]
    public class CollectionConnectionManagerFixture : ICollectionFixture<ConnectionManagerFixture> { }
    public class ConnectionManagerFixture
    {
        public ConnectionManagerFixture()
        {
            DatabaseHelper.RecreateSqlDatabase("ConnectionManager");
        }
    }

}
