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
using System.Globalization;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CsvSourceIdentityColumnTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");

        public CsvSourceIdentityColumnTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void IdentityAtPosition1()
        {
            var saveCulture = CultureInfo.CurrentCulture;
            try
            {
                //Arrange
                var dest4Columns =
                    new FourColumnsTableFixture("CsvDestination4Columns", identityColumnIndex: 0);
                var dest = new DbDestination<string[]>(Connection, "CsvDestination4Columns");
                var source = new CsvSource<string[]>("res/CsvSource/ThreeColumnsNoId.csv");

                if (source.CurrentCulture != null) CultureInfo.CurrentCulture = source.CurrentCulture;

                //Act
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();

                //Assert
                dest4Columns.AssertTestData();
            }
            finally
            {
                CultureInfo.CurrentCulture = saveCulture;
            }
        }

        [Fact]
        public void IdentityInTheMiddle()
        {
            var saveCulture = CultureInfo.CurrentCulture;
            try
            {
                //Arrange
                var dest4Columns =
                    new FourColumnsTableFixture("CsvDestination4Columns", identityColumnIndex: 2);
                var dest = new DbDestination<string[]>(Connection, "CsvDestination4Columns");
                var source = new CsvSource<string[]>("res/CsvSource/ThreeColumnsNoId.csv");
                if (source.CurrentCulture != null) CultureInfo.CurrentCulture = source.CurrentCulture;

                //Act
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();

                //Assert
                dest4Columns.AssertTestData();
            }
            finally
            {
                CultureInfo.CurrentCulture = saveCulture;
            }
        }


        [Fact]
        public void IdentityAtTheEnd()
        {
            var saveCulture = CultureInfo.CurrentCulture;
            try
            {
                //Arrange
                var dest4Columns =
                    new FourColumnsTableFixture("CsvDestination4Columns", identityColumnIndex: 3);
                var dest = new DbDestination<string[]>(Connection, "CsvDestination4Columns");

                //Act
                var source = new CsvSource<string[]>("res/CsvSource/ThreeColumnsNoId.csv");
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();

                //Assert
                dest4Columns.AssertTestData();
            }
            finally
            {
                CultureInfo.CurrentCulture = saveCulture;
            }
        }
    }
}