using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Reflection;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;


namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// A lookup task - data from the input can be enriched with data retrieved from the lookup source.
    /// </summary>
    /// <typeparam name="TInput">Type of data input and output</typeparam>
    /// <typeparam name="TSourceOutput">Type of lookup data</typeparam>
    public class LookupTransformation<TInput, TSourceOutput>
        : DataFlowTransformation<TInput, TInput>, ITask, IDataFlowTransformation<TInput, TInput>
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = "Lookup";
        public List<TSourceOutput> LookupData { get; set; }

        /* Public Properties */
        public override ISourceBlock<TInput> SourceBlock => RowTransformation.SourceBlock;
        public override ITargetBlock<TInput> TargetBlock => RowTransformation.TargetBlock;
        public IDataFlowSource<TSourceOutput> Source
        {
            get
            {
                return _source;
            }
            set
            {
                _source = value;
                Source.SourceBlock.LinkTo(LookupBuffer, new DataflowLinkOptions() { PropagateCompletion = true });
            }
        }

        public Func<TInput, TInput> TransformationFunc
        {
            get
            {
                return _rowTransformationFunc;
            }
            set
            {
                _rowTransformationFunc = value;
                InitRowTransformation(LoadLookupData);
            }
        }

        /* Private stuff */
        private ActionBlock<TSourceOutput> LookupBuffer { get; set; }
        private RowTransformation<TInput, TInput> RowTransformation { get; set; }
        private Func<TInput, TInput> _rowTransformationFunc;
        private IDataFlowSource<TSourceOutput> _source;
        private LookupTypeInfo TypeInfo { get; set; }

        public LookupTransformation()
        {
            LookupBuffer = new ActionBlock<TSourceOutput>(row => FillBuffer(row));
            if (_rowTransformationFunc == null)
                InitLookupWithMatchRetrieveAttributes();
        }

        public LookupTransformation(IDataFlowSource<TSourceOutput> lookupSource) : this()
        {
            Source = lookupSource;
        }

        public LookupTransformation(IDataFlowSource<TSourceOutput> lookupSource, Func<TInput, TInput> transformationFunc)
            : this(lookupSource)
        {
            TransformationFunc = transformationFunc;
        }

        public LookupTransformation(IDataFlowSource<TSourceOutput> lookupSource, Func<TInput, TInput> transformationFunc, List<TSourceOutput> lookupList)
            : this(lookupSource, transformationFunc)
        {
            LookupData = lookupList;
        }

        private void InitRowTransformation(Action initAction)
        {
            RowTransformation = new RowTransformation<TInput, TInput>(this, _rowTransformationFunc);
            RowTransformation.InitAction = initAction;
        }

        private void InitLookupWithMatchRetrieveAttributes()
        {
            _rowTransformationFunc = row => FindRowByAttributes(row);
            InitRowTransformation(() =>
            {
                ReadAndCheckTypeInfo();
                LoadLookupData();
            });
        }

        private void ReadAndCheckTypeInfo()
        {
            TypeInfo = new LookupTypeInfo(typeof(TInput), typeof(TSourceOutput));
            if (TypeInfo.MatchColumns.Count == 0 || TypeInfo.RetrieveColumns.Count == 0)
                throw new ETLBoxException("Please define either a transformation function or use the MatchColumn / RetrieveColumn attributes.");
        }

        private TInput FindRowByAttributes(TInput row)
        {
            var lookupHit = LookupData.Find(e =>
            {
                bool same = true;
                foreach (var mc in TypeInfo.MatchColumns)
                {
                    same &= mc.PropInInput.GetValue(row).Equals(mc.PropInOutput.GetValue(e));
                    if (!same) break;
                }
                return same;
            });
            if (lookupHit != null)
            {
                foreach (var rc in TypeInfo.RetrieveColumns)
                {
                    var retrieveValue = rc.PropInOutput.GetValue(lookupHit);
                    rc.PropInInput.TrySetValue(row, retrieveValue);
                }
            }
            return row;
        }

        private void LoadLookupData()
        {
            CheckLookupObjects();
            try
            {
                Source.Execute();
                LookupBuffer.Completion.Wait();
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        private void CheckLookupObjects()
        {
            if (Source == null) throw new ETLBoxException("You need to define a lookup source before using a LookupTransformation in a data flow");
        }

        private void FillBuffer(TSourceOutput sourceRow)
        {
            if (LookupData == null) LookupData = new List<TSourceOutput>();
            LookupData.Add(sourceRow);
        }

        public void LinkLookupSourceErrorTo(IDataFlowLinkTarget<ETLBoxError> target) =>
            Source.LinkErrorTo(target);

        public void LinkLookupTransformationErrorTo(IDataFlowLinkTarget<ETLBoxError> target) =>
            RowTransformation.LinkErrorTo(target);
    }

    /// <summary>
    /// A lookup task - data from the input can be enriched with data retrieved from the lookup source.
    /// The non generic implementation uses a dynamic object as input and lookup source.
    /// </summary>
    public class LookupTransformation : LookupTransformation<ExpandoObject, ExpandoObject>
    {
        public LookupTransformation() : base()
        { }

        public LookupTransformation(IDataFlowSource<ExpandoObject> lookupSource)
            : base(lookupSource)
        { }

        public LookupTransformation(IDataFlowSource<ExpandoObject> lookupSource, Func<ExpandoObject, ExpandoObject> transformationFunc)
            : base(lookupSource, transformationFunc)
        { }

        public LookupTransformation(IDataFlowSource<ExpandoObject> lookupSource, Func<ExpandoObject, ExpandoObject> transformationFunc, List<ExpandoObject> lookupList)
            : base(lookupSource, transformationFunc, lookupList)
        { }
    }

}
