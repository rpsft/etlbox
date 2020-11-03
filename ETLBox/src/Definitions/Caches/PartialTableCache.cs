using ETLBox.Connection;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public class PartialTableCache<TInput, TCache> : ICacheManager<TInput, TCache> where TCache : class
    {
        bool WasInitialized;
        public ICollection<TCache> Records => LookupBuffer.Data;
        public PartialTableCache()
        {
            //Memory = new List<TCache>();
        }
        //public List<TCache> Memory { get; set; }
        public bool Contains(TInput row)
        {
            return LookupBuffer.Data.FindFirst(cr => cr.Equals(row)) != null;
        }

        int id = 1;
        //public Action<T, IList<T>> FillCache { get; set; } 
        public void Add(TInput row)
        {
            var source = new DbSource<TCache>();
            source.ConnectionManager = connMan;
            source.Sql = $"SELECT Col1, Col2, Col3, Col4 FROM {TableName} Where Col1 = {id}";
            id++;
            LookupBuffer =  new MemoryDestination<TCache>();
            
            //Source.Sql = $"SELECT * FROM {TableName}";
            source.LinkTo(LookupBuffer);
            source.Execute();
            LookupBuffer.Wait();
        }
        MemoryDestination<TCache> LookupBuffer = new MemoryDestination<TCache>();

        public string TableName { get; set; }

        public IConnectionManager connMan { get; set; }
        
        //public DbSource<TCache> Source { get; set; }

        public TCache Find(TInput row)
        {
            var copy = row as TCache;
            return LookupBuffer.Data.FindFirst(cr => row.Equals(copy));
        }

        public void Init() { }
    }
}
