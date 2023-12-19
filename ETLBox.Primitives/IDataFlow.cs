using System.Collections.Generic;
using System.Dynamic;

namespace ETLBox.Primitives
{
    public interface IDataFlow
    {
        IDataFlowSource<ExpandoObject> Source { get; set; }

        IList<IDataFlowDestination<ExpandoObject>> Destinations { get; set; }

        IList<IDataFlowDestination<ETLBoxError>> ErrorDestinations { get; set; }
    }
}
