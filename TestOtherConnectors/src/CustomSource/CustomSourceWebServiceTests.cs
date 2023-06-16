using System.Net.Http;
using ALE.ETLBox.ControlFlow;

namespace TestOtherConnectors.CustomSource
{
    public class CustomSourceWebServiceTests : OtherConnectorsTestBase
    {
        public CustomSourceWebServiceTests(OtherConnectorsDatabaseFixture fixture)
            : base(fixture) { }

        /// <summary>
        /// See https://jsonplaceholder.typicode.com/ for details of the rest api
        /// used for this test
        /// </summary>
        [Fact]
        public void CustomSourceWithWebService()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(
                SqlConnection,
                "Create test table",
                @"CREATE TABLE dbo.WebServiceDestination 
                ( Id INT NOT NULL, UserId INT NOT NULL, Title NVARCHAR(100) NOT NULL, Completed BIT NOT NULL )"
            );
            DbDestination<Todo> dest = new DbDestination<Todo>(
                SqlConnection,
                "dbo.WebServiceDestination"
            );
            WebserviceReader wsreader = new WebserviceReader();

            //Act
            CustomSource<Todo> source = new CustomSource<Todo>(
                wsreader.ReadTodo,
                wsreader.EndOfData
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(5, RowCountTask.Count(SqlConnection, "dbo.WebServiceDestination"));
        }

        [Serializable]
        public class Todo
        {
            public int Id { get; set; }
            public int UserId { get; set; }
            public string Title { get; set; }
            public bool Completed { get; set; }
        }

        [Serializable]
        public class WebserviceReader
        {
            public string Json { get; set; }
            public int TodoCounter { get; set; } = 1;

            public Todo ReadTodo()
            {
                var todo = new Todo();
                using var httpClient = new HttpClient();
                var uri = new Uri("https://jsonplaceholder.typicode.com/todos/" + TodoCounter);
                TodoCounter++;
                var response = httpClient.GetStringAsync(uri).Result;
                JsonConvert.PopulateObject(response, todo);
                return todo;
            }

            public bool EndOfData()
            {
                return TodoCounter > 5;
            }
        }
    }
}