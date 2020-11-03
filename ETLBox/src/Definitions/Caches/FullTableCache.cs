using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBox.Exceptions;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public class FullTableCache<TInput, TCache> : ILookupCacheManager<TInput, TCache> 
        where TCache : class
    {
        public LookupTransformation<TInput, TCache> Lookup { get; set; }
        public ICollection<TCache> Records => LookupBuffer.Data;
        public FullTableCache()
        {
            //Memory = new List<TCache>();
        }
        //public List<TCache> Memory { get; set; }
        public bool Contains(TInput row)
        {
            return true;
        }

        //public Action<T, IList<T>> FillCache { get; set; } 
        public void Add(TInput row)
        {
            throw new ETLBoxException("Nothing to add - all data was already loaded");
        }


        MemoryDestination<TCache> LookupBuffer = new MemoryDestination<TCache>();


        public IDataFlowExecutableSource<TCache> Source { get; set; }

        //public TCache Find(TInput row)
        //{
        //    var copy = row as TCache;
        //    return LookupBuffer.Data.FindFirst(cr => row.Equals(copy));
        //}
        
        public void Init() {
            Source.LinkTo(LookupBuffer);
            Source.Execute();
            LookupBuffer.Wait();
        }
    }
}
