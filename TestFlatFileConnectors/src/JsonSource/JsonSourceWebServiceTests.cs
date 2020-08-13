using Castle.DynamicProxy.Generators.Emitters.SimpleAST;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using Moq;
using Moq.Protected;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;
using WireMock.Server;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class JsonSourceWebServiceTests
    {
        public JsonSourceWebServiceTests()
        {
        }

        public class Todo
        {
            [JsonProperty("Id")]
            public int Key { get; set; }
            public string Title { get; set; }
        }


        [Fact]
        public void JsonFromWebServiceWithGet()
        {
            // Arrange
            var server = WireMockServer.Start();
            server
                .Given(Request.Create().WithPath("/test").UsingGet())
                .RespondWith(
                    Response.Create()
                        .WithStatusCode(200)
                        .WithHeader("Content-Type", "text/json")
                        .WithBody(File.ReadAllText("res/JsonSource/Todos.json"))
                );
            var port = server.Ports.First();

            //Arrange
            MemoryDestination<Todo> dest = new MemoryDestination<Todo>();

            //Act
            JsonSource<Todo> source = new JsonSource<Todo>(@$"http://localhost:{port}/test");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.All(dest.Data, item => Assert.True(item.Key > 0));
            Assert.Equal(5, dest.Data.Count);
        }

        [Fact]
        public void GetPaginatedRequest()
        {
            // Arrange
            var server = WireMockServer.Start();
            server
                .Given(Request.Create().WithPath("/test1").UsingPost().WithBody("TestContent"))
                .RespondWith(
                    Response.Create()
                        .WithStatusCode(200)
                        .WithHeader("Content-Type", "text/json")
                        .WithBody(File.ReadAllText("res/JsonSource/Todos_Page1.json"))
                );
            server
                .Given(Request.Create().WithPath("/test2").UsingPost().WithBody("TestContent"))
                .RespondWith(
                    Response.Create()
                        .WithStatusCode(200)
                        .WithHeader("Content-Type", "text/json")
                        .WithBody(File.ReadAllText("res/JsonSource/Todos_Page2.json"))
                );
            var port = server.Ports.First();

            //Arrange
            MemoryDestination<Todo> dest = new MemoryDestination<Todo>();

            //Act
            JsonSource<Todo> source = new JsonSource<Todo>();
            int i = 1;
            source.GetNextUri = smd => @$"http://localhost:{port}/test{i++}";
            source.HasNextUri = smd => i <= 2;
            source.HttpRequestMessage.Content = new ByteArrayContent(Encoding.UTF8.GetBytes("TestContent"));
            source.HttpRequestMessage.Method = HttpMethod.Post;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.All(dest.Data, item => Assert.True(item.Key > 0));
            Assert.Equal(4, dest.Data.Count);
        }

        [Fact]
        public void JsonFromWebServiceWithPost()
        {
            // Arrange
            var server = WireMockServer.Start();
            server
                .Given(Request.Create()
                                .WithPath("/testpost")
                                .UsingPost()
                                .WithBody("TestContent")
                        )
                .RespondWith(
                    Response.Create()
                        .WithStatusCode(200)
                        .WithHeader("Content-Type", "text/json")
                        .WithBody(File.ReadAllText("res/JsonSource/Todos.json"))
                );
            var port = server.Ports.First();

            //Arrange
            MemoryDestination<Todo> dest = new MemoryDestination<Todo>();
            JsonSource<Todo> source = new JsonSource<Todo>(@$"http://localhost:{port}/testpost");

            //Act
            source.HttpRequestMessage.Method = HttpMethod.Post;
            source.HttpRequestMessage.Content = new ByteArrayContent(Encoding.UTF8.GetBytes("TestContent"));
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.All(dest.Data, item => Assert.True(item.Key > 0));
            Assert.Equal(5, dest.Data.Count);
        }

        [Fact]
        public void JsonFromWebServiceWith500()
        {
            // Arrange
            var server = WireMockServer.Start();
            server
                .Given(Request.Create()
                                .WithPath("/testerror")
                                .UsingPut()
                        )
                .RespondWith(
                    Response.Create()
                        .WithStatusCode(500)
                );
            var port = server.Ports.First();

            //Arrange
            MemoryDestination<Todo> dest = new MemoryDestination<Todo>();
            JsonSource<Todo> source = new JsonSource<Todo>(@$"http://localhost:{port}/testerror");
            source.HttpRequestMessage.Method = HttpMethod.Put;

            //Act & Assert
            Assert.ThrowsAny<Exception>(() =>
            {
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();
            });
        }
    }
}
