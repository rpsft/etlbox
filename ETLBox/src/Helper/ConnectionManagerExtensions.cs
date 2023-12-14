using ALE.ETLBox.src.Definitions.ConnectionManager;

namespace ALE.ETLBox.src.Helper
{
    public static class ConnectionManagerExtensions
    {
        public static string FormatQuery(
            this IConnectionManager manager,
            FormattableString source
        ) => source.ToString(QueryFormatter.GetForConnection(manager));
    }
}
