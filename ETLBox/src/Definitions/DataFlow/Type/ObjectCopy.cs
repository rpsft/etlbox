using ALE.ETLBox.Helper;

namespace ALE.ETLBox.DataFlow
{
    internal class ObjectCopy<TInput>
    {
        internal TypeInfo TypeInfo { get; set; }

        public ObjectCopy(TypeInfo typeInfo)
        {
            TypeInfo = typeInfo;
        }

        internal TInput Clone(TInput row)
        {
            TInput clone;
            if (TypeInfo.IsArray)
            {
                Array source = row as Array;
                clone = (TInput)Activator.CreateInstance(typeof(TInput), source.Length);
                Array dest = clone as Array;
                Array.Copy(source, dest, source.Length);
            }
            else if (TypeInfo.IsDynamic)
            {
                clone = (TInput)Activator.CreateInstance(typeof(TInput));

                var original = (IDictionary<string, object>)row;
                var _clone = (IDictionary<string, object>)clone;

                foreach (var kvp in original)
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
