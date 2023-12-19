using System.Threading.Tasks;

namespace ETLBox.Primitives
{
    public interface IDataFlowDestination<in TInput> : IDataFlowLinkTarget<TInput>
    {
        void Wait();
        Task Completion { get; }
    }
}
