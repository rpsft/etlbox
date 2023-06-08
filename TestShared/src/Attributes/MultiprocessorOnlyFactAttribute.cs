using System;
using Xunit;

namespace TestShared.Attributes
{
    public sealed class MultiprocessorOnlyFactAttribute : FactAttribute
    {
        public MultiprocessorOnlyFactAttribute()
        {
            if (Environment.ProcessorCount < 2)
            {
                Skip = "Ignore: parallel tests cannot run during single processor execution";
            }
        }
    }
}
