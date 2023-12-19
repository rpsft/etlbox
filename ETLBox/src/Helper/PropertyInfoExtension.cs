namespace ALE.ETLBox.Helper
{
    public static class PropertyInfoExtension
    {
        public static void SetValueOrThrow(this PropertyInfo pi, object obj, object value)
        {
            if (pi.CanWrite)
                pi.SetValue(obj, value);
            else
                throw new ETLBoxException(
                    $"Can't write into property {pi.Name} - property has no setter definition."
                );
        }

        public static void TrySetValue(
            this PropertyInfo pi,
            object obj,
            object value,
            Type enumType = null
        )
        {
            if (!pi.CanWrite)
            {
                return;
            }

            if (enumType != null && value != null && enumType.IsEnum)
            {
                pi.SetValue(obj, Enum.Parse(enumType, value.ToString()));
            }
            else
            {
                pi.SetValue(obj, value);
            }
        }
    }
}
