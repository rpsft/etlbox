using System;
using System.Collections.Generic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public class MemoryCache<TInput, TCache> : ICacheManager<TInput, TCache>
        where TCache : class
    {
        public ICollection<TCache> Records => Cache;     
        
        public Func<TInput, TCache,bool> CompareFunc { get; set; }

        public bool Contains(TInput row)
        {            
            bool result = false;
            if (CompareFunc != null)
                result = Cache.Find(cr => CompareFunc.Invoke(row, cr)) != null;
            else
                result = Cache.Find(cr => cr.Equals(row)) != null;            
            return result;
        }

        public void Add(TInput row)
        {
            ObjectCopy<TInput> objectCopy = new ObjectCopy<TInput>(TypeInfo);
            var copy = objectCopy.Clone(row) as TCache;
            
            if (copy != null)
                Cache.Add(copy);
            if (MaxCacheSize > 0 && Cache.Count > MaxCacheSize)
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
