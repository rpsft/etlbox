using ALE.ETLBox.Common;
using ALE.ETLBox.Common.DataFlow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Define your own destination block. The non generic implementation uses a dynamic object as input.
    /// </summary>
    [PublicAPI]
    public class CustomDestination : CustomDestination<ExpandoObject>
    {
        public CustomDestination() { }

        public CustomDestination(Action<ExpandoObject> writeAction)
            : base(writeAction) { }
    }
}
