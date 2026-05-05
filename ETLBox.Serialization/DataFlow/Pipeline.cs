using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Xml.Linq;
using ALE.ETLBox.Common.DataFlow;
using ETLBox.Primitives;
using JetBrains.Annotations;

namespace ALE.ETLBox.Serialization.DataFlow;

/// <summary>
/// Wraps a sequence of data-flow steps into a single transformation block.
/// Each child step is linked to the next; <see cref="LinkErrorTo"/> is forwarded to all
/// internal steps that support it.
/// </summary>
/// <typeparam name="TIn">Input type of the first step.</typeparam>
/// <typeparam name="TOut">Output type of the last step.</typeparam>
[PublicAPI]
public class Pipeline<TIn, TOut> : DataFlowTransformation<TIn, TOut>, IDataFlowXmlSerializable
{
    /// <inheritdoc />
    public override string TaskName { get; set; } = "Pipeline";

    /// <inheritdoc />
    public override ITargetBlock<TIn> TargetBlock => TransformBlock;

    /// <inheritdoc />
    public override ISourceBlock<TOut> SourceBlock => TransformBlock;

    private readonly List<object> _steps = new();

    /// <summary>All steps added via <see cref="ReadSteps"/>.</summary>
    public IReadOnlyList<object> Steps => _steps;

    /// <summary>Appends a step to the internal step list.</summary>
    protected void AddStep(object step) => _steps.Add(step);

    /// <summary>First step (target side of the encapsulated block).</summary>
    protected IDataFlowLinkTarget<TIn>? Head;

    /// <summary>Last step (source side of the encapsulated block).</summary>
    protected IDataFlowLinkSource<TOut>? Tail;

    /// <summary>
    /// Wires <paramref name="head"/> and <paramref name="tail"/> as the entry and exit
    /// of this pipeline and encapsulates them into a single propagator block.
    /// </summary>
    protected void SetHeadAndTail(IDataFlowLinkTarget<TIn> head, IDataFlowLinkSource<TOut> tail)
    {
        Head = head;
        Tail = tail;
        TransformBlock = DataflowBlock.Encapsulate(head.TargetBlock, tail.SourceBlock);
    }

    /// <summary>
    /// Forwards <see cref="LinkErrorTo"/> to every internal step that supports it.
    /// </summary>
    public override void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target)
    {
        foreach (var step in _steps)
            if (step is ILinkErrorSource src)
                src.LinkErrorTo(target);
    }

    /// <summary>
    /// Processes <paramref name="children"/> starting at <paramref name="startIndex"/>,
    /// linking each step to the next and appending them to <see cref="Steps"/>.
    /// Method-like child elements (<c>LinkTo</c>, <c>LinkErrorTo</c>) are collected and
    /// dispatched via reflection on <c>this</c> only after <see cref="SetHeadAndTail"/> is
    /// called, so that <see cref="DataFlowTransformation{TInput,TOutput}.SourceBlock"/> is
    /// already initialised when those methods run.
    /// </summary>
    protected void ReadSteps(IList<XElement> children, int startIndex, IDataFlowXmlContext context)
    {
        var stepsStartIndex = _steps.Count;
        var pendingMethodElements = new List<XElement>();
        object? lastStep = null;
        Type? lastOutputType = null;
        object? lastSourceStep = null;

        for (var i = startIndex; i < children.Count; i++)
        {
            var child = children[i];

            if (HasMatchingMethod(child))
            {
                pendingMethodElements.Add(child);
                continue;
            }

            var step = CreateAndValidateStep(child, lastOutputType, context);
            _steps.Add(step);

            if (lastStep != null && lastOutputType != null)
                LinkSteps(lastStep, lastOutputType, step);

            var outputType = GetLinkSourceOutputType(step);
            if (outputType != null)
            {
                lastOutputType = outputType;
                lastSourceStep = step;
            }
            lastStep = step;
        }

        if (_steps.Count > stepsStartIndex)
            FinalizeHeadAndTail(stepsStartIndex, lastSourceStep);

        foreach (var element in pendingMethodElements)
            TryInvokeXmlMethod(element, context);
    }

    private static object CreateAndValidateStep(
        XElement child,
        Type? lastOutputType,
        IDataFlowXmlContext context
    )
    {
        if (child.Elements("LinkTo").Any() || child.Elements("LinkErrorTo").Any())
            throw new InvalidDataException(
                $"Step '{child.Name.LocalName}' inside <Pipeline> must not contain "
                    + "<LinkTo> or <LinkErrorTo>. Route links at the <Pipeline> level."
            );

        var step =
            context.CreateObject(child.Name.LocalName, child)
            ?? throw new InvalidOperationException(
                $"Could not create step '{child.Name.LocalName}' inside <Pipeline>."
            );

        if (lastOutputType != null && !ImplementsLinkTarget(step, lastOutputType))
            throw new InvalidDataException(
                $"Type mismatch at '{child.Name.LocalName}': "
                    + $"expected IDataFlowLinkTarget<{lastOutputType.Name}>"
            );

        return step;
    }

    private void FinalizeHeadAndTail(int stepsStartIndex, object? lastSourceStep)
    {
        var head =
            _steps[stepsStartIndex] as IDataFlowLinkTarget<TIn>
            ?? throw new InvalidDataException(
                $"First step must implement IDataFlowLinkTarget<{typeof(TIn).Name}>"
            );
        var tail =
            lastSourceStep as IDataFlowLinkSource<TOut>
            ?? throw new InvalidDataException(
                $"No step implements IDataFlowLinkSource<{typeof(TOut).Name}>"
            );
        SetHeadAndTail(head, tail);
    }

    private bool HasMatchingMethod(XElement element) =>
        Array.Exists(
            GetType().GetMethods(),
            m =>
                m.Name == element.Name.LocalName
                && m.GetParameters().Length == 1
                && !m.IsGenericMethodDefinition
        );

    /// <inheritdoc />
    public virtual void ReadXml(XElement element, IDataFlowXmlContext context)
    {
        var children = element.Elements().ToList();
        if (children.Count == 0)
            return;
        ReadSteps(children, 0, context.WithoutGlobalActions());
    }

    private static bool ImplementsLinkTarget(object step, Type itemType) =>
        step.GetType()
            .GetInterfaces()
            .Any(i =>
                i.IsGenericType
                && i.GetGenericTypeDefinition() == typeof(IDataFlowLinkTarget<>)
                && i.GetGenericArguments()[0] == itemType
            );

    private static Type? GetLinkSourceOutputType(object step) =>
        step.GetType()
            .GetInterfaces()
            .FirstOrDefault(i =>
                i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDataFlowLinkSource<>)
            )
            ?.GetGenericArguments()[0];

    /// <summary>
    /// If a method named <c>element.Name.LocalName</c> with exactly one non-generic parameter
    /// exists on <c>this</c>, creates each child element as an object via
    /// <paramref name="context"/> and invokes the method. Returns <c>false</c> when no matching
    /// method is found (element is a pipeline step, not a method call).
    /// </summary>
    private void TryInvokeXmlMethod(XElement element, IDataFlowXmlContext context)
    {
        var method = GetType()
            .GetMethods()
            .FirstOrDefault(m =>
                m.Name == element.Name.LocalName
                && m.GetParameters().Length == 1
                && !m.IsGenericMethodDefinition
            );
        if (method is null)
            return;

        foreach (var childElement in element.Elements())
        {
            var target =
                context.CreateObject(childElement.Name.LocalName, childElement)
                ?? throw new InvalidOperationException(
                    $"Could not create '{childElement.Name.LocalName}' for method '{element.Name.LocalName}'."
                );
            method.Invoke(this, new[] { target });
        }
    }

    /// <summary>
    /// Links <paramref name="source"/> to <paramref name="target"/> at the TPL Dataflow level
    /// (type-erased via reflection) and registers the source's completion in the target's
    /// predecessor list so that <c>CheckCompleteAction</c> waits correctly.
    /// </summary>
    private static void LinkSteps(object source, Type itemType, object target)
    {
        var linkSourceType = typeof(IDataFlowLinkSource<>).MakeGenericType(itemType);
        var linkTargetType = typeof(IDataFlowLinkTarget<>).MakeGenericType(itemType);

        var sourceBlock = linkSourceType.GetProperty("SourceBlock")!.GetValue(source)!;
        var targetBlock = linkTargetType.GetProperty("TargetBlock")!.GetValue(target)!;

        // Find DataflowBlock.LinkTo<T>(ISourceBlock<T>, ITargetBlock<T>) — the 2-param overload
        var linkToMethod = typeof(DataflowBlock)
            .GetMethods()
            .First(m =>
                m.Name == "LinkTo" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2
            )
            .MakeGenericMethod(itemType);
        linkToMethod.Invoke(null, new[] { sourceBlock, targetBlock });

        var completion = (Task)
            typeof(IDataflowBlock).GetProperty("Completion")!.GetValue(sourceBlock)!;
        linkTargetType
            .GetMethod("AddPredecessorCompletion")!
            .Invoke(target, new object[] { completion });
    }
}

/// <summary>
/// <see cref="ExpandoObject"/> pipeline that also acts as a data-flow source when an
/// internal source is provided as its first child element.
/// </summary>
[PublicAPI]
public sealed class Pipeline
    : Pipeline<ExpandoObject, ExpandoObject>,
        IDataFlowSource<ExpandoObject>
{
    private IDataFlowSource<ExpandoObject>? _source;
    private bool _outputBound;

    /// <inheritdoc />
    public void Execute(CancellationToken cancellationToken)
    {
        EnsureOutputBound();
        _source?.Execute(cancellationToken);
    }

    /// <inheritdoc />
    public Task ExecuteAsync(CancellationToken cancellationToken)
    {
        EnsureOutputBound();
        return _source?.ExecuteAsync(cancellationToken)
            ?? throw new InvalidOperationException(
                "Pipeline has no internal source. Drive it by linking an external source."
            );
    }

    /// <inheritdoc />
    public override IDataFlowLinkSource<ExpandoObject> LinkTo(
        IDataFlowLinkTarget<ExpandoObject> target
    )
    {
        _outputBound = true;
        return base.LinkTo(target);
    }

    /// <inheritdoc />
    public override IDataFlowLinkSource<ExpandoObject> LinkTo(
        IDataFlowLinkTarget<ExpandoObject> target,
        Predicate<ExpandoObject> predicate
    )
    {
        _outputBound = true;
        return base.LinkTo(target, predicate);
    }

    /// <inheritdoc />
    public override IDataFlowLinkSource<ExpandoObject> LinkTo(
        IDataFlowLinkTarget<ExpandoObject> target,
        Predicate<ExpandoObject> rowsToKeep,
        Predicate<ExpandoObject> rowsIntoVoid
    )
    {
        _outputBound = true;
        return base.LinkTo(target, rowsToKeep, rowsIntoVoid);
    }

    /// <inheritdoc />
    public override IDataFlowLinkSource<TConvert> LinkTo<TConvert>(
        IDataFlowLinkTarget<ExpandoObject> target
    )
    {
        _outputBound = true;
        return base.LinkTo<TConvert>(target);
    }

    /// <inheritdoc />
    public override IDataFlowLinkSource<TConvert> LinkTo<TConvert>(
        IDataFlowLinkTarget<ExpandoObject> target,
        Predicate<ExpandoObject> predicate
    )
    {
        _outputBound = true;
        return base.LinkTo<TConvert>(target, predicate);
    }

    /// <inheritdoc />
    public override IDataFlowLinkSource<TConvert> LinkTo<TConvert>(
        IDataFlowLinkTarget<ExpandoObject> target,
        Predicate<ExpandoObject> rowsToKeep,
        Predicate<ExpandoObject> rowsIntoVoid
    )
    {
        _outputBound = true;
        return base.LinkTo<TConvert>(target, rowsToKeep, rowsIntoVoid);
    }

    private void EnsureOutputBound()
    {
        if (_outputBound || Tail is IDataFlowDestination<ExpandoObject>)
            return;
        if (Tail == null)
            return;

        var sink = new VoidDestination<ExpandoObject>();
        Tail.LinkTo(sink);
        _outputBound = true;
    }

    /// <inheritdoc />
    public override void ReadXml(XElement element, IDataFlowXmlContext context)
    {
        var children = element.Elements().ToList();
        if (children.Count == 0)
            return;

        var innerContext = context.WithoutGlobalActions();
        var stepStart = 0;
        var firstType = innerContext.ResolveType(children[0].Name.LocalName);
        if (firstType != null && typeof(IDataFlowSource<ExpandoObject>).IsAssignableFrom(firstType))
        {
            _source =
                innerContext.CreateObject(children[0].Name.LocalName, children[0])
                    as IDataFlowSource<ExpandoObject>
                ?? throw new InvalidDataException(
                    $"'{children[0].Name.LocalName}' resolved as source type but could not be "
                        + "cast to IDataFlowSource<ExpandoObject>."
                );
            AddStep(_source);
            stepStart = 1;
        }

        if (stepStart < children.Count)
            ReadSteps(children, stepStart, innerContext);

        // Raw TPL link so _source's completion registers in Pipeline's PredecessorCompletions,
        // not in Head's — prevents Head from closing before an external upstream finishes.
        if (_source != null && Head != null)
        {
            _source.SourceBlock.LinkTo(Head.TargetBlock);
            AddPredecessorCompletion(_source.SourceBlock.Completion);
        }
    }
}
