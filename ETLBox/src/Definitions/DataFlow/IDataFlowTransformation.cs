namespace ETLBox.DataFlow
{
    /// <summary>
    /// Implemented by transformations that have one or more inputs of the same type and one or more outputs of the same type
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data</typeparam>
    /// <typeparam name="TOutput">Type of outgoing data</typeparam>
    public interface IDataFlowTransformation<TInput, TOutput> : IDataFlowSource<TOutput>, IDataFlowDestination<TInput>
    {

    }

    /// <summary>
    /// Implemented by transformations that can have multiple inputs with different type and one or more outputs of the same type
    /// </summary>
    /// <typeparam name="TOutput">Type of outgoing data</typeparam>
    public interface IDataFlowTransformation<TOutput> : IDataFlowSource<TOutput>, IDataFlowDestination
    {

    }
}
