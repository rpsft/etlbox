using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;
using ALE.ETLBox.Common.DataFlow;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Filters data rows based on a predicate function.
    /// Rows passing the predicate are forwarded, others are discarded.
    /// </summary>
    /// <typeparam name="TInput">Type of data input and output</typeparam>
    [PublicAPI]
    public class RowFiltration<TInput> : DataFlowTransformation<TInput, TInput>
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = "Filter rows.";

        /* Public Properties */
        public override ISourceBlock<TInput> SourceBlock => TransformBlock;
        public override ITargetBlock<TInput> TargetBlock => TransformBlock;

        /// <summary>
        /// Predicate function that determines whether a row should pass through.
        /// Returns true to keep the row, false to discard it.
        /// </summary>
        public Func<TInput, bool> PredicateFunc { get; set; }

        /* Constructors */
        public RowFiltration(Func<TInput, bool> predicateFunc)
            : this()
        {
            PredicateFunc = predicateFunc;
        }

        public RowFiltration()
            : this(logger: null) { }

        /// <summary>
        /// Creates a new instance with an injected logger.
        /// </summary>
        public RowFiltration([CanBeNull] ILogger<RowFiltration<TInput>> logger)
            : base(logger)
        {
            TransformBlock = new TransformManyBlock<TInput, TInput>(
                (Func<TInput, IEnumerable<TInput>>)FilterRow
            );
        }

        private IEnumerable<TInput> FilterRow(TInput row)
        {
            if (row is null)
                return Array.Empty<TInput>();
            try
            {
                return PredicateFunc.Invoke(row) ? [row] : Array.Empty<TInput>();
            }
            catch (Exception e)
            {
                if (!ErrorHandler.HasErrorBuffer)
                    throw;
                ErrorHandler.Send(e, ErrorHandler.ConvertErrorData(row));
                return Array.Empty<TInput>();
            }
        }
    }

    /// <summary>
    /// Filters data rows based on a predicate function.
    /// Non-generic version working with ExpandoObject.
    /// </summary>
    /// <see cref="RowFiltration{TInput}"/>
    [PublicAPI]
    public class RowFiltration : RowFiltration<ExpandoObject>
    {
        public RowFiltration() { }

        /// <summary>
        /// Creates a new instance with an injected logger.
        /// </summary>
        public RowFiltration(ILogger<RowFiltration> logger)
            : base(logger) { }

        public RowFiltration(Func<ExpandoObject, bool> predicateFunc)
            : base(predicateFunc) { }
    }
}
