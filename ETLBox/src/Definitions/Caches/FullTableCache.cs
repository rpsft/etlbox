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
        public ICollection<TCache> Records => LookupBuffer.Data;        

        public bool Contains(TInput row)
        {
            return true;
        }

        public void Add(TInput row)
        {
            throw new ETLBoxException("Nothing to add - all data was already loaded");
        }

        public void Init()
        {
            Source.LinkTo(LookupBuffer);            
            Source.Execute();
            LookupBuffer.Wait();
        }

        public LookupTransformation<TInput, TCache> Lookup { get; set; }

        public FullTableCache()
        {
        }

        IDataFlowExecutableSource<TCache> Source => Lookup.Source;
        MemoryDestination<TCache> LookupBuffer = new MemoryDestination<TCache>();
    }
}
