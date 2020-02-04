using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CSVSourceIdentityColumTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CSVSourceIdentityColumTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void IdentityAtPosition1()
        {
            //Arrange
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("CSVDestination4Columns", identityColumnIndex: 0);
            DBDestination dest = new DBDestination(Connection, "CSVDestination4Columns");

            //Act
            CsvSource source = new CsvSource("res/CSVSource/ThreeColumnsNoId.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }

        [Fact]
        public void IdentityInTheMiddle()
        {
            //Arrange
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("CSVDestination4Columns", identityColumnIndex: 2);
            DBDestination dest = new DBDestination(Connection, "CSVDestination4Columns");

            //Act
            CsvSource source = new CsvSource("res/CSVSource/ThreeColumnsNoId.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }


        [Fact]
        public void IdentityAtTheEnd()
        {
            //Arrange
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("CSVDestination4Columns", identityColumnIndex: 3);
            DBDestination dest = new DBDestination(Connection, "CSVDestination4Columns");

            //Act
            CsvSource source = new CsvSource("res/CSVSource/ThreeColumnsNoId.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }
    }
}
