using System;
using System.Dynamic;
using System.Globalization;
using CsvHelper.Configuration;

namespace ALE.ETLBox.Serialization.DataFlow;

public static class DataFlowActivator
{
    public static object? CreateInstance(Type type)
    {
        // Special cases for library classes without default constructors
        if (type == typeof(CsvConfiguration))
        {
            return new CsvConfiguration(CultureInfo.InvariantCulture);
        }

        var constructedType = type;
        if (type.IsGenericType)
        {
            constructedType = type.MakeGenericType(typeof(ExpandoObject));
        }

        return Activator.CreateInstance(constructedType);
    }
}
