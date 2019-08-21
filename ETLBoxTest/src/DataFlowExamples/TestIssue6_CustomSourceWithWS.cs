using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Net.Http;

namespace ALE.ETLBoxTest {
    [TestClass]
    public class TestIssue6_CustomSourceWithWS {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void ClassInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
        }

        [TestInitialize]
        public void TestInit() {
        }

        /// <summary>
        /// See https://jsonplaceholder.typicode.com/ for details of the rest api
        /// used for this test
        /// </summary>
        /// </summary>
        [TestMethod]
        public void CustomSourceWithWebService() {
            SqlTask.ExecuteNonQuery("Create test table",
                @"CREATE TABLE dbo.ws_dest 
                ( Id INT NOT NULL, UserId INT NOT NULL, Title NVARCHAR(100) NOT NULL, Completed BIT NOT NULL )"
            );

            WebserviceReader wsreader = new WebserviceReader();
            CustomSource<Todo> source = new CustomSource<Todo>(wsreader.ReadTodo, wsreader.EndOfData);

            DBDestination<Todo> dest = new DBDestination<Todo>("dbo.ws_dest");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
        }

        public class Todo
        {
            public int Id { get; set; }
            public int UserId { get; set; }
            public string Title { get; set; }
            public bool Completed { get; set; }
        }

        public class WebserviceReader
        {
            public string Json { get; set; }
            public int TodoCounter { get; set; } = 1;
            public Todo ReadTodo()
            {
                var todo = new Todo();
                using (var httpClient = new HttpClient())
                {
                    var uri = new Uri("https://jsonplaceholder.typicode.com/todos/" + TodoCounter);
                    TodoCounter++;
                    var response = httpClient.GetStringAsync(uri).Result;
                    Newtonsoft.Json.JsonConvert.PopulateObject(response, todo);

                }
                return todo;
            }

            public bool EndOfData()
            {
                return TodoCounter > 5;

            }
        }

    }

}
