//using ALE.ETLBox;
//using ALE.ETLBox.ConnectionManager;
//using ALE.ETLBox.ControlFlow;
//using ALE.ETLBox.DataFlow;
//using ALE.ETLBox.Helper;
//using ALE.ETLBox.Logging;
//using Newtonsoft.Json;
//using System;
//using System.Collections.Concurrent;
//using System.Collections.Generic;
//using System.IO;
//using System.Linq;
//using System.Threading.Tasks;
//using Xunit;

//namespace ALE.ETLBoxTests
//{
//    public class MergeableRowTests
//    {
//        public class MergeableTestRow : MergeableRow
//        {
//            [IdColumn]
//            public int ColKey1 { get; set; }
//            [IdColumn]
//            public string ColKey2 { get; set; }
//            [CompareColumn]
//            public string ColValue1 { get; set; }
//            [CompareColumn]
//            public double ColValue2 { get; set; }
//        }

//        [Fact]
//        public void CheckUniqueColumnProp()
//        {
//            //Arrange
//            MergeableTestRow row = new MergeableTestRow()
//            {
//                ColKey1 = 1,
//                ColKey2 = "A",
//                ColValue1 = "X",
//                ColValue2 = 3.0
//            };

//            //Act
//            string id = row.UniqueId;

//            //Assert
//            Assert.True(id == "1A" || id == "A1");
//        }

//        [Fact]
//        public void CheckUniqueColumnPropInParallel()
//        {
//            //Arrange
//            ConcurrentBag<int> ids = new ConcurrentBag<int>();
//            List<int> objectNumbers = new List<int>() { 1, 2, 3, 4, 5, 6, 7, 8 };
//            Parallel.ForEach(objectNumbers, new ParallelOptions() { MaxDegreeOfParallelism = 4 },
//                number =>
//                {
//                    MergeableTestRow row = new MergeableTestRow()
//                    {
//                        ColKey1 = number,
//                        ColKey2 = "",
//                        ColValue1 = "X",
//                        ColValue2 = 3.0
//                    };
//                    //Act
//                    ids.Add(int.Parse(row.UniqueId));
//                });

//            //Assert
//            Assert.All(ids, id => Assert.True(id >= 1 && id <= 8));
//        }

//        [Fact]
//        public void CheckIfObjectsAreEqual()
//        {
//            //Arrange
//            MergeableTestRow row = new MergeableTestRow()
//            {
//                ColKey1 = 1,
//                ColKey2 = "A",
//                ColValue1 = "X",
//                ColValue2 = 3.0
//            };

//            MergeableTestRow other = new MergeableTestRow()
//            {
//                ColKey1 = 2,
//                ColKey2 = "B",
//                ColValue1 = "X",
//                ColValue2 = 3.0
//            };

//            //Act
//            bool isEqual = row.Equals(other);

//            //Assert
//            Assert.True(isEqual);
//        }

//        [Fact]
//        public void CheckIfObjectsAreNotEqual()
//        {
//            //Arrange
//            MergeableTestRow row = new MergeableTestRow()
//            {
//                ColKey1 = 1,
//                ColKey2 = "A",
//                ColValue1 = "X",
//                ColValue2 = 3.0
//            };

//            MergeableTestRow other = new MergeableTestRow()
//            {
//                ColKey1 = 2,
//                ColKey2 = "B",
//                ColValue1 = "X2",
//                ColValue2 = 3.0
//            };

//            //Act
//            bool isEqual = row.Equals(other);

//            //Assert
//            Assert.False(isEqual);
//        }


//    }
//}
