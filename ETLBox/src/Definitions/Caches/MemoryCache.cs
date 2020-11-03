using System;
using System.Collections.Generic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public class MemoryCache<TInput, TCache> : ICacheManager<TInput, TCache>
        where TCache : class
    {
        public ICollection<TCache> Records => Memory;
        public MemoryCache()
        {
            Memory = new List<TCache>();
        }
        public List<TCache> Memory { get; set; }
        public bool Contains(TInput row)
        {
            return Memory.Find(cr => cr.Equals(row)) != null;
        }

        public void Add(TInput row)
        {
            ObjectCopy<TInput> objectCopy = new ObjectCopy<TInput>(TypeInfo);
            var copy = objectCopy.Clone(row) as TCache;
            
            if (copy != null)
                Memory.Add(copy);
            if (Memory.Count > MaxCacheSize)
                Memory.RemoveAt(0);
        }

        public int MaxCacheSize { get; set; } = DEFAULT_MAX_CACHE_SIZE;

        public const int DEFAULT_MAX_CACHE_SIZE = 10000;
        TypeInfo TypeInfo;
        public void Init() {
            TypeInfo = new TypeInfo(typeof(TInput)).GatherTypeInfo();
        }
    }
}
