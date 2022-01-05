using System.Runtime.InteropServices;
using Xunit;

namespace TestShared.Attributes
{
    public sealed class WindowsOnlyFactAttribute : FactAttribute
    {
        public WindowsOnlyFactAttribute() {
            if(!RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) {
                Skip = "Ignore on non-Windows";
            }
        }
    }
}