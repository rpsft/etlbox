using ALE.ETLBox.src.Definitions.DataFlow;

namespace ALE.ETLBox.DataFlow
{
    public interface ILinkErrorSource
    {
        void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target);
    }
}
