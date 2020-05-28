using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace ALE.ETLBox.DataFlow
{
    internal class ObjectCopy<TInput>
    {
        internal TypeInfo TypeInfo { get; set; }
        public ObjectCopy(TypeInfo typeInfo)
        {
            this.TypeInfo = typeInfo;
        }
        internal TInput Clone(TInput row)
        {
            TInput clone = default(TInput);
            if (TypeInfo.IsArray)
            {
                Array source = row as Array;
                clone = (TInput)Activator.CreateInstance(typeof(TInput), new object[] { source.Length });
                Array dest = clone as Array;
                Array.Copy(source, dest, source.Length);
            }
            else if (TypeInfo.IsDynamic)
            {
                clone = (TInput)Activator.CreateInstance(typeof(TInput));

                var _original = (IDictionary<string, object>)row;
                var _clone = (IDictionary<string, object>)clone;

                foreach (var kvp in _original)
                    _clone.Add(kvp);
            }
            else
            {
                clone = (TInput)Activator.CreateInstance(typeof(TInput));
                foreach (PropertyInfo propInfo in TypeInfo.Properties)
                {
                    propInfo.TrySetValue(clone, propInfo.GetValue(row));
                }
            }

            return clone;
        }
    }

}
