using ETLBox.DataFlow.Transformations;
using System;
using System.Collections.Generic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public interface ICacheManager<TInput, TCache>
    {
        ICollection<TCache> Records { get; }
        bool Contains(TInput row);
        void Add(TInput row);
        void Init();
    }  

    public interface ILookupCacheManager<TInput, TCache> : ICacheManager<TInput, TCache>
        where TCache :class
    {
        LookupTransformation<TInput, TCache> Lookup { get; set; }
    }
}
