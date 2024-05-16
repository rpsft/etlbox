using ALE.ETLBox.Helper;
using TypeInfo = ALE.ETLBox.Common.DataFlow.TypeInfo;

namespace ALE.ETLBox.DataFlow
{
    internal sealed class ObjectCopy<TInput>
    {
        private TypeInfo TypeInfo { get; }

        public ObjectCopy(TypeInfo typeInfo)
        {
            TypeInfo = typeInfo;
        }

        internal TInput Clone(TInput row)
        {
            TInput clone;
            if (TypeInfo.IsArray)
            {
                var source = row as Array;
                clone = (TInput)Activator.CreateInstance(typeof(TInput), source!.Length);
                var dest = clone as Array;
                Array.Copy(source, dest!, source.Length);
            }
            else if (TypeInfo.IsDynamic)
            {
                clone = (TInput)Activator.CreateInstance(typeof(TInput));

                var original = (IDictionary<string, object>)row;
                var dictionary = (IDictionary<string, object>)clone;

                foreach (var keyValuePair in original)
                    dictionary.Add(keyValuePair);
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
