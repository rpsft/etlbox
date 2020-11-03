using System;
using System.Collections.Generic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public class MemoryCache<TInput, TCache> : ICacheManager<TInput, TCache>
        where TCache : class
    {
        public ICollection<TCache> Records => Cache;                

        public bool Contains(TInput row)
        {
            return Cache.Find(cr => cr.Equals(row)) != null;
        }

        public void Add(TInput row)
        {
            ObjectCopy<TInput> objectCopy = new ObjectCopy<TInput>(TypeInfo);
            var copy = objectCopy.Clone(row) as TCache;
            
            if (copy != null)
                Cache.Add(copy);
            if (Cache.Count > MaxCacheSize)
                Cache.RemoveAt(0);
        }

        public void Init()
        {
            TypeInfo = new TypeInfo(typeof(TInput)).GatherTypeInfo();
        }

        
        public int MaxCacheSize { get; set; } = DEFAULT_MAX_CACHE_SIZE;

        public const int DEFAULT_MAX_CACHE_SIZE = 10000;

        public MemoryCache()
        {
        }

        TypeInfo TypeInfo;
        List<TCache> Cache = new List<TCache>();
    }
}
