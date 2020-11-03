using ETLBox.Connection;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public class PartialDbTableCache<TInput, TCache> : ILookupCacheManager<TInput, TCache> where TCache : class
    {
        public LookupTransformation<TInput, TCache> Lookup { get; set; }
        public ICollection<TCache> Records => Cache;

        public void Init() { }

        public bool Contains(TInput row)
        {
            var res = Cache.Find(cr => Lookup.PartialCacheSettings.MatchFunc(row, cr));
            return res != null;
        }

        public void Add(TInput row)
       {
            var templateSource = Lookup.Source as DbSource<TCache>;
            var source = new DbSource<TCache>();
            source.ConnectionManager = templateSource.ConnectionManager;
            source.Sql = Lookup.PartialCacheSettings.LoadCacheSql.Invoke(row);
            source.ColumnNames = templateSource.ColumnNames;
            source.SourceTableDefinition = templateSource.SourceTableDefinition;
            
            PartialLoadDest = new MemoryDestination<TCache>();
            PartialLoadDest.Data = Cache;            
            source.LinkTo(PartialLoadDest);
            source.Execute();
            PartialLoadDest.Wait();

        }

        public PartialDbTableCache()
        {

        }

        MemoryDestination<TCache> PartialLoadDest = new MemoryDestination<TCache>();
        List<TCache> Cache = new List<TCache>();
    }
}
