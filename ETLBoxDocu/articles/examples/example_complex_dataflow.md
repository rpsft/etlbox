# Example Data Flow

## Preparations
First, you need to set up a database. You can use the Controlflow Tasks to do this.

```C#
ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;"));
DropDatabaseTask.Delete("DemoDB"); 
CreateDatabaseTask.Create("DemoDB");
ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;Initial Catalog=DemoDB"));
CreateSchemaTask.Create("demo");
```

Now, we need some tables to have some source and destination for our data flow. 

```C#
TableDefinition OrderDataTableDef = new TableDefinition("demo.Orders",
    new List<TableColumn>() {
        new TableColumn("OrderKey", "int",allowNulls: false, isPrimaryKey:true, isIdentity:true),
        new TableColumn("Number","nvarchar(100)", allowNulls: false),
        new TableColumn("Item","nvarchar(200)", allowNulls: false),
        new TableColumn("Amount","money", allowNulls: false),
        new TableColumn("CustomerKey","int", allowNulls: false)
    });

TableDefinition CustomerTableDef = new TableDefinition("demo.Customer",
    new List<TableColumn>() {
        new TableColumn("CustomerKey", "int",allowNulls: false, isPrimaryKey:true, isIdentity:true),
        new TableColumn("Name","nvarchar(200)", allowNulls: false),                
    });

TableDefinition CustomerRatingTableDef = new TableDefinition("demo.CustomerRating",
    new List<TableColumn>() {
        new TableColumn("RatingKey", "int",allowNulls: false, isPrimaryKey:true, isIdentity:true),
        new TableColumn("CustomerKey", "int",allowNulls: false),
        new TableColumn("TotalAmount","decimal(10,2)", allowNulls: false),
        new TableColumn("Rating","nvarchar(3)", allowNulls: false)                
    });

OrderDataTableDef.CreateTable();
CustomerTableDef.CreateTable();
CustomerRatingTableDef.CreateTable();
SqlTask.ExecuteNonQuery("Fill customer table", "INSERT INTO demo.Customer values('Sandra Kettler')");
SqlTask.ExecuteNonQuery("Fill customer table", "INSERT INTO demo.Customer values('Nick Thiemann')");
SqlTask.ExecuteNonQuery("Fill customer table", "INSERT INTO demo.Customer values('Zoe Rehbein')");
SqlTask.ExecuteNonQuery("Fill customer table", "INSERT INTO demo.Customer values('Margit Gries')");
```

Let us define some POCOs (Plain old component objects), so we have something to describe our data that goes into our pipeline.

```C#
public class Order {            
    public string Number { get; set; }
    public string Item { get; set; }
    public decimal Amount { get; set; }            
    public int CustomerKey { get; set; }
    public string CustomerName { get; set; }            
    public CustomerRating Rating { get; set; }
}

public class Customer {
    public int CustomerKey { get; set; }
    public string CustomerName { get; set; }            
}

public class CustomerRating {
    public int CustomerKey { get; set; }
    public decimal TotalAmount { get; set; }
    public string Rating { get; set; }
}
```

## Build the pipeline

Now we can construct a pipeline. Let's start with the source

```C#
CSVSource sourceOrderData = new CSVSource("DataFlow/DemoData.csv");
sourceOrderData.Delimiter = ";";
```

Source data will look like this:

```csv
OrderNumber;OrderItem;OrderAmount;CustomerName
4711;Yellow Shoes;30.00$;Sandra Kettler
4712;Green T-Shirt;14.99$;Nick Thiemann
4713;Blue Jeans;29.99$;Zoe Rehbein
4714;White Jeans;29.99$;Margit Gries
4807;Green Shoes;32.00$;Margit Gries
```
...


We add a row transformation - and connect it with source. Data will be read from the source and moved into the row transformation. A row transformation will go through each row and modificates it by a given function. Furthermore, a row transformation can change the object type of the input into something different - we use this to transform our string array into our POCO. 

```C#
RowTransformation<string[], Order> transIntoObject = new RowTransformation<string[], Order>(CSVIntoObject);    sourceOrderData.LinkTo(transIntoObject);

private Order CSVIntoObject(string[] csvLine) {
    return new Order() {
        Number = csvLine[0],
        Item = csvLine[1],
        Amount = decimal.Parse(csvLine[2].Substring(0, csvLine[2].Length - 1), CultureInfo.GetCultureInfo("en-US")),
        CustomerName = csvLine[3]
    };
}
```

No we define another source from the database - we need this for our Lookup. A Lookup will use a third source to enrich the data in the flow, e.g. to add some kind of classification to the data.

```C#
DBSource<Customer> sourceCustomerData = new DBSource<Customer>(CustomerTableDef);
LookupCustomerKey lookupCustKeyClass = new LookupCustomerKey();
Lookup<Order, Order, Customer> lookupCustomerKey = new Lookup<Order, Order, Customer>(
        lookupCustKeyClass.FindKey, sourceCustomerData, lookupCustKeyClass.LookupData);            
transIntoObject.LinkTo(lookupCustomerKey);

public class LookupCustomerKey {

    public List<Customer> LookupData { get; set; } = new List<Customer>();

    public Order FindKey(Order orderRow) {
        var customer = LookupData.Where(cust => cust.CustomerName == orderRow.CustomerName).FirstOrDefault();
        orderRow.CustomerKey = customer?.CustomerKey ?? 0;
        return orderRow;
    }
}
```

Now we add a multicast - a multicast "double" the input into 2 same outputs with the exact same data. This is useful if you want to have additional destination populated with data based on your input. 

```C#
Multicast<Order> multiCast = new Multicast<Order>();
lookupCustomerKey.LinkTo(multiCast);
```

One output of the multicast will end up in a database destination. 

```C#
DBDestination<Order> destOrderTable = new DBDestination<Order>(OrderDataTableDef);
multiCast.LinkTo(destOrderTable);
```

The other output will go into a block transformation. A Block Transformation is a blocking pipeline element - it will wait until all input data arrived at the block transformation. Then it will apply the given function on all items in the element. When done, it will continue to hand over the data to the next element in the pipeline. 

```C#
BlockTransformation<Order> blockOrders = new BlockTransformation<Order>(BlockTransformOrders);
multiCast.LinkTo(blockOrders);

private List<Order> BlockTransformOrders(List<Order> allOrders) {
    List<int> allCustomerKeys = allOrders.Select(ord => ord.CustomerKey).Distinct().ToList();
    foreach (int custKey in allCustomerKeys) {
        var firstOrder = allOrders.Where(ord => ord.CustomerKey == custKey).FirstOrDefault();
        firstOrder.Rating = new CustomerRating();
        firstOrder.Rating.CustomerKey = custKey;
        firstOrder.Rating.TotalAmount = allOrders.Where(ord => ord.CustomerKey == custKey).Sum(ord => ord.Amount);
        firstOrder.Rating.Rating = firstOrder.Rating.TotalAmount > 50 ? "A" : "F";                    
    }
    return allOrders;
}
```

Now we want to transform this data from the Block transformation into another object type - we use a RowTransformation for this. But this will only happen for particular data - depending if the Rating could be calculated. If the Rating was found to be invalid (null), we write the data somewhere else (without storing it).

```C#
DBDestination<CustomerRating> destRating = new DBDestination<CustomerRating>(CustomerRatingTableDef);
RowTransformation<Order, CustomerRating> transOrderIntoCust = new RowTransformation<Order, CustomerRating>(OrderIntoRating);
CustomDestination<Order> destSink = new CustomDestination<Order>(row => {; });
blockOrders.LinkTo(transOrderIntoCust, ord => ord.Rating != null);
blockOrders.LinkTo(destSink, ord => ord.Rating == null);
transOrderIntoCust.LinkTo(destRating);

private CustomerRating OrderIntoRating(Order orderRow) {
    return new CustomerRating() {
        CustomerKey = orderRow.CustomerKey,
        TotalAmount = orderRow.Rating.TotalAmount,
        Rating = orderRow.Rating.Rating
    };
}
```

Finally, we need to start the data flow. All sources need to be started, and then we wait until all destinations got the completion message from the source.

```C#
sourceOrderData.ExecuteAsync();
destOrderTable.Wait();
destRating.Wait();
```






