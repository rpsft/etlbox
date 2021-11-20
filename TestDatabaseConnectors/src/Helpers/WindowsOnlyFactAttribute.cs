using System;
using System.Runtime.InteropServices;
using Xunit;

namespace TestDatabaseConnectors.Helpers
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