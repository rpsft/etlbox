using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace ALE.ETLBoxDemo {
    public class DataFlowTasks {
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


        public void Preparation() {
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;"));
            DropDatabaseTask.Drop("DemoDB");
            CreateDatabaseTask.Create("DemoDB");
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;Initial Catalog=DemoDB"));
            CreateSchemaTask.Create("demo");
            OrderDataTableDef.CreateTable();
            CustomerTableDef.CreateTable();
            CustomerRatingTableDef.CreateTable();
            SqlTask.ExecuteNonQuery("Fill customer table", "INSERT INTO demo.Customer values('Sandra Kettler')");
            SqlTask.ExecuteNonQuery("Fill customer table", "INSERT INTO demo.Customer values('Nick Thiemann')");
            SqlTask.ExecuteNonQuery("Fill customer table", "INSERT INTO demo.Customer values('Zoe Rehbein')");
            SqlTask.ExecuteNonQuery("Fill customer table", "INSERT INTO demo.Customer values('Margit Gries')");
        }

        public class Order {
            public string Number { get; set; }
            public string Item { get; set; }
            public decimal Amount { get; set; }
            public int CustomerKey { get; set; }
            public string CustomerName { get; set; }
            public Rating Rating { get; set; }
        }

        public class Customer {
            public int CustomerKey { get; set; }
            public string CustomerName { get; set; }
        }

        public class Rating {
            public int CustomerKey { get; set; }
            public decimal TotalAmount { get; set; }
            public string RatingValue { get; set; }
        }

        public class LookupCustomerKey {

            public List<Customer> LookupData { get; set; } = new List<Customer>();

            public Order FindKey(Order orderRow) {
                var customer = LookupData.Where(cust => cust.CustomerName == orderRow.CustomerName).FirstOrDefault();
                orderRow.CustomerKey = customer?.CustomerKey ?? 0;
                return orderRow;
            }
        }


        public void Start() {
            CSVSource sourceOrderData = new CSVSource("src/DataFlow/DemoData.csv");
            sourceOrderData.Configuration.Delimiter = ";";
            RowTransformation<string[], Order> transIntoObject = new RowTransformation<string[], Order>(CSVIntoObject);
            DBSource<Customer> sourceCustomerData = new DBSource<Customer>(CustomerTableDef);
            LookupCustomerKey lookupCustKeyClass = new LookupCustomerKey();
            Lookup<Order, Order, Customer> lookupCustomerKey = new Lookup<Order, Order, Customer>(
                lookupCustKeyClass.FindKey, sourceCustomerData, lookupCustKeyClass.LookupData);

            Multicast<Order> multiCast = new Multicast<Order>();
            DBDestination<Order> destOrderTable = new DBDestination<Order>(OrderDataTableDef);

            BlockTransformation<Order> blockOrders = new BlockTransformation<Order>(BlockTransformOrders);
            DBDestination<Rating> destRating = new DBDestination<Rating>(CustomerRatingTableDef);
            RowTransformation<Order, Rating> transOrderIntoCust = new RowTransformation<Order, Rating>(OrderIntoRating);
            VoidDestination<Order> destSink = new VoidDestination<Order>();

            sourceOrderData.LinkTo(transIntoObject);
            transIntoObject.LinkTo(lookupCustomerKey);

            lookupCustomerKey.LinkTo(multiCast);
            multiCast.LinkTo(destOrderTable);

            multiCast.LinkTo(blockOrders);
            blockOrders.LinkTo(transOrderIntoCust, ord => ord.Rating != null);
            blockOrders.LinkTo(destSink, ord => ord.Rating == null);
            transOrderIntoCust.LinkTo(destRating);

            sourceOrderData.ExecuteAsync();
            destOrderTable.Wait();
            destRating.Wait();
        }

        private Order CSVIntoObject(string[] csvLine) {
            return new Order() {
                Number = csvLine[0],
                Item = csvLine[1],
                Amount = decimal.Parse(csvLine[2].Substring(0, csvLine[2].Length - 1), CultureInfo.GetCultureInfo("en-US")),
                CustomerName = csvLine[3]
            };
        }

        private List<Order> BlockTransformOrders(List<Order> allOrders) {
            List<int> allCustomerKeys = allOrders.Select(ord => ord.CustomerKey).Distinct().ToList();
            foreach (int custKey in allCustomerKeys) {
                var firstOrder = allOrders.Where(ord => ord.CustomerKey == custKey).FirstOrDefault();
                firstOrder.Rating = new Rating();
                firstOrder.Rating.CustomerKey = custKey;
                firstOrder.Rating.TotalAmount = allOrders.Where(ord => ord.CustomerKey == custKey).Sum(ord => ord.Amount);
                firstOrder.Rating.RatingValue = firstOrder.Rating.TotalAmount > 50 ? "A" : "F";
            }
            return allOrders;
        }

        private Rating OrderIntoRating(Order orderRow) {
            return new Rating() {
                CustomerKey = orderRow.CustomerKey,
                TotalAmount = orderRow.Rating.TotalAmount,
                RatingValue = orderRow.Rating.RatingValue
            };
        }




    }
}
