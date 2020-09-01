using ETLBox.Exceptions;
using System;
using System.Reflection;

namespace ETLBox.Helper
{
    /// <summary>
    /// Reflection helper class that allows to directly set values in properties.
    /// </summary>
    public static class PropertyInfoExtension
    {
        /// <summary>
        /// Sets a value in a property. If this is not possible, this method throws an exception.
        /// </summary>
        /// <param name="pi">The property info for the property</param>
        /// <param name="obj">The object that contains the property</param>
        /// <param name="value">The new value for the property</param>
        public static void SetValueOrThrow(this PropertyInfo pi, object obj, object value)
        {
            if (pi.CanWrite)
                pi.SetValue(obj, value);
            else
                throw new ETLBoxException($"Can't write into property {pi?.Name} - property has no setter definition.");
        }

        /// <summary>
        /// Tries to set a value in a property. If not possible, it will do nothing.
        /// </summary>
        /// <param name="pi">The property info for the property</param>
        /// <param name="obj">The object that contains the property</param>
        /// <param name="value">The new value for the property</param>
        /// <param name="enumType">If the property is an enum type, this will need special handling - pass the enum type here. Default value is null.</param>
        public static void TrySetValue(this PropertyInfo pi, object obj, object value, Type enumType = null)
        {
            if (pi.CanWrite)
            {
                if (enumType != null && value != null && enumType.IsEnum)
                {
                    pi.SetValue(obj, Enum.Parse(enumType, value?.ToString()));
                }
                else
                {
                    pi.SetValue(obj, value);
                }
            }
        }
    }
}
