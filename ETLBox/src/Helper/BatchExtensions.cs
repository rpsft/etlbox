using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace ETLBox.Helper
{
    public static class BatchLinq
    {
        public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> source, int size)
        {
            if (size <= 0)
                throw new ArgumentOutOfRangeException("size", "Must be greater than zero.");
            using (var enumerator = source.GetEnumerator())
                while (enumerator.MoveNext())
                {
                    int i = 0;
                    // Batch is a local function closing over `i` and `enumerator` that
                    // executes the inner batch enumeration
                    IEnumerable<T> Batch()
                    {
                        do yield return enumerator.Current;
                        while (++i < size && enumerator.MoveNext());
                    }

                    yield return Batch();
                    while (++i < size && enumerator.MoveNext()) ; // discard skipped items
                }
        }
    }
}
