using System.Runtime.InteropServices;

namespace TestShared.Attributes
{
    public sealed class WindowsOnlyTheoryAttribute : TheoryAttribute
    {
        public WindowsOnlyTheoryAttribute()
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Skip = "Ignore on non-Windows";
            }
        }
    }
}
