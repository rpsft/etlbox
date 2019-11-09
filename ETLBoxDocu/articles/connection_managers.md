# Connection managers 

## ADO.NET Connection pooling

First of all, please let me clarify something about the Open() method of the connection manager: 
Whenever this method is called (doesn't matter if this happens on a completely new object or the object 
already exists), it will use instantiate a new connection on the underlying ADO.NET provider. 
It will also call the open method on this provider. But this won't always create a new database connection. 
The method naming Open could be misleading here - it actually should be something like 'OpenOrReuseConnectionFromPool. The reason for that is that ADO.NET (which the whole connection handling is based upon) has it's own connection pooling. Whenever you try to open a connection (which includes opening up the right port, doing a handshake, authentification etc.), the ADO.NET connection pooling will check if it makes sense to open up a new connection or if it could reuse an existing - already opened - connection from the pool. Whenever you call the Close()` method (which is totally recommended when using ADO.NET),this won't automatically close the connection. It will just return the connection to the pool - it will stay open until ADO.NET decides that it not needed anymore.

Please see here for more details of connection pooling: https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-connection-pooling

Thus having said, I don't think that calling the open / close (and also recreating the object every time) is actually a performance hit. It will just use the connection pooling provided - as recommended by ADO.NET, which probably will perform already very well. So if you would create a test environment where you open up a lot of connections to the database, you would see that the connection pool would have only one connection which stays open. So calling Open and Close multiple times in a row would in most cases never open up more than one connection. I also have a Test regarding the this pooling behavior in my test project:

etlbox/TestsETLBox/src/DataFlowTests/ConnectionManager/SqlConnectionManagerTests.cs

Lines 64 to 76 in 9135059
 [Fact] 
 public void TestOpeningConnectionTwice() 
 { 
     SqlConnectionManager con = new SqlConnectionManager(new ConnectionString(ConnectionStringParameter)); 
     AssertOpenConnectionCount(0, ConnectionStringParameter); 
     con.Open(); 
     con.Open(); 
     AssertOpenConnectionCount(1, ConnectionStringParameter); 
     con.Close(); 
     AssertOpenConnectionCount(1, ConnectionStringParameter); 
     SqlConnection.ClearAllPools(); 
     AssertOpenConnectionCount(0, ConnectionStringParameter); 
 } 

In this test class you will also find other scenarios testing the ADO.NET pooling.

TL;DR; - the current implementation follows the recommendation of Microsoft and makes use of the connection pooling provided by ADO.NET. This means that this actually can increase your performance, and in most scenarios you never have more connections open that you actually need for your application.