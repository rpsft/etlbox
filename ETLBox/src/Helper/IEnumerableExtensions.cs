using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace ETLBox.Helper
{
    internal static class IEnumerableExtensions
    {
        public static T FindFirst<T>(this IEnumerable<T> source, Func<T, bool> condition)
        {
            foreach (T item in source)
                if (condition(item))
                    return item;
            return default(T);
        }
    }
}
