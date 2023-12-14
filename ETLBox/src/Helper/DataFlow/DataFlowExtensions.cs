using CsvHelper.Configuration;

namespace ALE.ETLBox.Helper.DataFlow
{
    public static class DataFlowExtensions
    {
        public static CsvConfiguration Create()
            => new(CultureInfo.InvariantCulture);

        public static MethodInfo GetMethod(Type type)
        {
            return Array.Find(typeof(DataFlowExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static),
                m => m.ReturnParameter.ParameterType == type);
        }
    }
}
