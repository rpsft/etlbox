namespace ETLBox.Primitives
{
    public interface ILinkErrorSource
    {
        void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target);
    }
}
