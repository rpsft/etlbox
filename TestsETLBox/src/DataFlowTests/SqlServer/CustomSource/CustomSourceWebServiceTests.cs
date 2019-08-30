using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests.SqlServer
{
    [Collection("Sql Server DataFlow")]
    public class CustomSourceWebServiceTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public CustomSourceWebServiceTests(DatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        /// <summary>
        /// See https://jsonplaceholder.typicode.com/ for details of the rest api
        /// used for this test
        /// </summary>
        [Fact]
        public void CustomSourceWithWebService()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(Connection, "Create test table",
                @"CREATE TABLE dbo.WebServcieDestination 
                ( Id INT NOT NULL, UserId INT NOT NULL, Title NVARCHAR(100) NOT NULL, Completed BIT NOT NULL )"
            );
            DBDestination<Todo> dest = new DBDestination<Todo>(Connection, "dbo.WebServcieDestination");
            WebserviceReader wsreader = new WebserviceReader();

            //Act
            CustomSource<Todo> source = new CustomSource<Todo>(wsreader.ReadTodo, wsreader.EndOfData);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(5, RowCountTask.Count(Connection, "dbo.WebServcieDestination"));

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
