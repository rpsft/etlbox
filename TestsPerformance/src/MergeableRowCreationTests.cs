using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace ALE.ETLBoxTests.Performance
{
    [Collection("Performance")]
    public class MergeableRowCreationTests
    {
        private readonly ITestOutputHelper output;

        public MergeableRowCreationTests(PerformanceDatabaseFixture dbFixture, ITestOutputHelper output)
        {
            this.output = output;
        }


        public class MergeableTestRow : MergeableRow
        {
            [IdColumn]
            public long ColKey1 { get; set; }
            [IdColumn]
            public string ColKey2 { get; set; }
            public string ColValue1 { get; set; }
            public string ColValue2 { get; set; }
        }

        public class MergeableTestHidingRefĺection : MergeableRow
        {
            [IdColumn]
            public long ColKey1 { get; set; }
            [IdColumn]
            public string ColKey2 { get; set; }
            public string ColValue1 { get; set; }
            public string ColValue2 { get; set; }
            public new string UniqueId => $"{ColKey1}{ColKey2}-hidesPropThatUsesReflection";
            public new bool Equals(object other)
            {
                var o = other as MergeableTestHidingRefĺection;
                if (o == null) return false;
                return ColValue1 == o.ColValue1 && ColValue2 == o.ColValue2;
            }
        }

        [Theory]
        [InlineData(2000000, 0.20)]
        public void CompareWithHiddenReflection(int objectsToCreate, double deviation)
        {
            //Arrange

            //Act
            var timeWithReflection = BigDataHelper.LogExecutionTime("Creation with Reflection",
             () =>
             {
                 for (int i = 0; i < objectsToCreate; i++)
                 {
                     MergeableTestRow row = new MergeableTestRow()
                     {
                         ColKey1 = i,
                         ColKey2 = "Test",
                         ColValue1 = "X1" + i,
                         ColValue2 = "T1" + i
                     };
                     //string id = row.UniqueId;
                     //bool isequal = row.Equals(row);
                     //LogTask.Trace("Id:" + id + " Equals:" + isequal.ToString());
                 };
             });
            output.WriteLine("Elapsed " + timeWithReflection.TotalSeconds + " seconds for creation with reflection.");

            var timeWithoutReflection = BigDataHelper.LogExecutionTime("Creation without Reflection",
             () =>
             {
                 for (int i = 0; i < objectsToCreate; i++)
                 {
                     MergeableTestHidingRefĺection row = new MergeableTestHidingRefĺection()
                     {
                         ColKey1 = i,
                         ColKey2 = "Test",
                         ColValue1 = "X2" + i,
                         ColValue2 = "T2" + i
                     };
                     //string id = row.UniqueId;
                     //bool isequal = row.Equals(row);
                     //LogTask.Trace("Id:" + id + " Equals:" + isequal.ToString());
                 }
             });
            output.WriteLine("Elapsed " + timeWithoutReflection.TotalSeconds + " seconds for creation without reflection.");

            //Assert
            Assert.True(timeWithoutReflection < timeWithReflection);
            Assert.True(timeWithoutReflection.TotalMilliseconds * (deviation+1) > timeWithReflection.TotalMilliseconds);
        }
    }
}
