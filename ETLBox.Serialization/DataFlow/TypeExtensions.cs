using System;
using System.Globalization;
using System.Linq;

namespace ALE.ETLBox.Serialization.DataFlow
{
    internal static class TypeExtensions
    {
        private static readonly Type[] s_nullableTypes =
        {
            typeof(string),
            typeof(DateTime?),
            typeof(Guid?),
            typeof(int?),
            typeof(long?),
            typeof(bool?),
            typeof(char?),
            typeof(byte?),
            typeof(short?),
            typeof(ushort?),
            typeof(ulong?),
            typeof(uint?)
        };

        public static bool IsNullable(this Type type)
        {
            return s_nullableTypes.Contains(type);
        }

        public static bool TryParse(this string value, Type type, out object objValue)
        {
            objValue = value;

            if (type == typeof(char) || type == typeof(char?))
            {
                objValue = char.Parse(value);
                return true;
            }

            if (type == typeof(byte) || type == typeof(byte?))
            {
                objValue = byte.Parse(value);
                return true;
            }

            if (type == typeof(short) || type == typeof(short?))
            {
                objValue = short.Parse(value);
                return true;
            }

            if (type == typeof(ushort) || type == typeof(ushort?))
            {
                objValue = ushort.Parse(value);
                return true;
            }

            if (type == typeof(int) || type == typeof(int?))
            {
                objValue = int.Parse(value);
                return true;
            }

            if (type == typeof(uint) || type == typeof(uint?))
            {
                objValue = uint.Parse(value);
                return true;
            }

            if (type == typeof(long) || type == typeof(long?))
            {
                objValue = long.Parse(value);
                return true;
            }

            if (type == typeof(ulong) || type == typeof(ulong?))
            {
                objValue = ulong.Parse(value);
                return true;
            }

            if (type == typeof(bool) || type == typeof(bool?))
            {
                objValue = bool.Parse(value);
                return true;
            }

            if (type == typeof(DateTime) || type == typeof(DateTime?))
            {
                objValue = DateTime.Parse(value, CultureInfo.CurrentCulture);
                return true;
            }

            if (type == typeof(Guid) || type == typeof(Guid?))
            {
                objValue = Guid.Parse(value);
                return true;
            }

            return false;
        }
    }
}
