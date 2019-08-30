//using ALE.ETLBox;
//using ALE.ETLBox.ConnectionManager;
//using ALE.ETLBox.ControlFlow;
//using ALE.ETLBox.DataFlow;
//using ALE.ETLBox.Logging;
//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using System.Collections.Generic;
//using System.Linq;

//namespace ALE.ETLBoxTest
//{
//    [TestClass]
//    public class TestIssue5_DuplicateCheck
//    {
//        public TestContext TestContext { get; set; }
//        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
//        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

//        [ClassInitialize]
//        public static void ClassInit(TestContext testContext)
//        {
//            TestHelper.RecreateDatabase(testContext);
//            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
//        }

//        [TestInitialize]
//        public void TestInit()
//        {
//            CleanUpSchemaTask.CleanUp("dbo");
//        }

//        public class Poco
//        {
//            public int ID { get; set; }
//            public string Name { get; set; }
//            [CsvHelper.Configuration.Attributes.Name("Text")]
//            public string Value { get; set; }

//            public bool IsDuplicate { get; set; }
//        }

//        [TestMethod]
//        public void TestDuplicateCheckInRowTrans()
//        {
//            CreateLogTablesTask.CreateLog();
//            DataFlow.LoggingThresholdRows = 2;
//            CSVSource<Poco> source = new CSVSource<Poco>("src/DataFlowExamples/Duplicate.csv");
//            source.Configuration.Delimiter = ";";
//            source.Configuration.TrimOptions = CsvHelper.Configuration.TrimOptions.Trim;
//            source.Configuration.MissingFieldFound = null;
//            List<int> IDs = new List<int>(); //at the end of the flow, this list will contain all IDs of your source
//            RowTransformation<Poco, Poco> rowTrans = new RowTransformation<Poco, Poco>(input =>
//             {
//                 if (IDs.Contains(input.ID))
//                     input.IsDuplicate = true;
//                 else
//                     IDs.Add(input.ID);
//                 return input;
//             });

//            var multicast = new Multicast<Poco>();


//            var dest = new DBDestination<Poco>("dbo.Staging");
//            TableDefinition stagingTable = new TableDefinition("dbo.Staging", new List<TableColumn>() {
//                new TableColumn("Key", "INT", allowNulls: false, isPrimaryKey:true, isIdentity:true),
//                new TableColumn("ID", "INT", allowNulls: false),
//                new TableColumn("Value", "NVARCHAR(100)", allowNulls: false),
//                new TableColumn("Name", "NVARCHAR(100)", allowNulls: false)
//            });
//            stagingTable.CreateTable();


//            var trash = new VoidDestination<Poco>();

//            source.LinkTo(rowTrans);
//            rowTrans.LinkTo(multicast);
//            multicast.LinkTo(dest, input => input.IsDuplicate == false);
//            multicast.LinkTo(trash, input => input.IsDuplicate == true);

//            source.Execute();
//            dest.Wait();
//            trash.Wait();
//        }


//        [TestMethod]
//        public void TestDuplicateCheckWithBlockTrans()
//        {
//            CSVSource<Poco> source = new CSVSource<Poco>("src/DataFlowExamples/Duplicate.csv");
//            source.Configuration.Delimiter = ";";
//            source.Configuration.TrimOptions = CsvHelper.Configuration.TrimOptions.Trim;
//            source.Configuration.MissingFieldFound = null;
//            List<int> IDs = new List<int>(); //at the end of the flow, this list will contain all IDs of your source
//            BlockTransformation<Poco> blockTrans = new BlockTransformation<Poco>(inputList =>
//            {
//                return inputList.GroupBy(item => item.ID).Select(y => y.First()).ToList();
//            });

//            var dest = new DBDestination<Poco>("dbo.Staging");
//            TableDefinition stagingTable = new TableDefinition("dbo.Staging", new List<TableColumn>() {
//                new TableColumn("Key", "INT", allowNulls: false, isPrimaryKey:true, isIdentity:true),
//                new TableColumn("ID", "INT", allowNulls: false),
//                new TableColumn("Value", "NVARCHAR(100)", allowNulls: false),
//                new TableColumn("Name", "NVARCHAR(100)", allowNulls: false)
//            });
//            stagingTable.CreateTable();

//            source.LinkTo(blockTrans);
//            blockTrans.LinkTo(dest);
//            source.Execute();
//            dest.Wait();
//        }


//    }
//}


