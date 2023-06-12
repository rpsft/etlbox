using System.Runtime.InteropServices;

namespace TestShared.Attributes
{
    public sealed class WindowsOnlyFactAttribute : FactAttribute
    {
        public WindowsOnlyFactAttribute()
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Skip = "Ignore on non-Windows";
            }
        }
    }
}
