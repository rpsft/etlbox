using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public class HashCache<TInput> : ICacheManager<TInput, HashValue>
        where TInput: class
    {
        public ICollection<HashValue> Records => Cache;             

        public Func<TInput, int> HashSumFunc { get; set; }
        public bool Contains(TInput row)
        {
            HashValue hashValue = new HashValue(HashSumFunc(row));//row.GetHashCode();
            //hashValue.Hash = CalculateHashForProperties(row, TypeInfo.Properties);
            return Cache.Contains(hashValue);
        }

        //private int CalculateHashForProperties(TInput row, PropertyInfo[] properties)
        //{
        //    int hash = 29;
        //    unchecked
        //    {                
        //        foreach (var prop in properties)
        //            hash = hash * 486187739 + (prop.GetValue(row)?.GetHashCode() ?? 17);
        //        return hash;
        //    }
        //}

        public void Add(TInput row)
        {
            HashValue hashValue = new HashValue(HashSumFunc(row));//row.GetHashCode();
            //hashValue.Hash = CalculateHashForProperties(row, TypeInfo.Properties);
            Cache.Add(hashValue);            
        }

        public void Init()
        {
            TypeInfo = new TypeInfo(typeof(TInput)).GatherTypeInfo();
        }

        public HashCache()
        {
        }

        TypeInfo TypeInfo;
        HashSet<HashValue> Cache = new HashSet<HashValue>();
                
    }

    public class HashValue
    {
        public readonly int Hash;

        public HashValue(int hash)
        {
            Hash = hash;
        }
        public override int GetHashCode() => Hash;

        public override bool Equals(object obj)
        {
            HashValue comp = obj as HashValue;
            if (comp == null) return false;
            return comp.Hash == this.Hash;
        }
    }
}
