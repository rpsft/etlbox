using System;
using System.Runtime.InteropServices;
using Xunit;

namespace TestDatabaseConnectors.Helpers
{
    public sealed class WindowsOnlyTheoryAttribute : TheoryAttribute
    {
        public WindowsOnlyTheoryAttribute() {
            if(!RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) {
                Skip = "Ignore on non-Windows";
            }
        }
    }
}