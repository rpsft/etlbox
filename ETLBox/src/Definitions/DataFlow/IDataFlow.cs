using ALE.ETLBox.src.Definitions.DataFlow;

namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlow
    {
        IDataFlowSource<ExpandoObject> Source { get; set; }

        IList<IDataFlowDestination<ExpandoObject>> Destinations { get; set; }

        IList<IDataFlowDestination<ETLBoxError>> ErrorDestinations { get; set; }
    }
}
