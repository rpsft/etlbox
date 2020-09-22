using ETLBox.DataFlow.Connectors;
using ETLBox.Exceptions;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// ColumnRename allows you to rename the column or properties names of your ingoing data. 
    /// This transformation works with objects, ExpandoObjects and arrays as input data type.    
    /// Provide a column mapping with the old and the new name. The mapping can also be automatically retrieved from 
    /// existing ColumnMap attributes. For arrays provide the array index and the new name. 
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data</typeparam>
    public class ColumnRename<TInput> : DataFlowTransformation<TInput, ExpandoObject>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = "ColumnRename";

        /// <inheritdoc/>
        public override ISourceBlock<ExpandoObject> SourceBlock => RowTransformation.SourceBlock;

        /// <inheritdoc/>
        public override ITargetBlock<TInput> TargetBlock => RowTransformation.TargetBlock;


        /// <summary>
        /// The column mapping defines how existing properties or columns are renamed. 
        /// For objects and dynamic object provide a mapping with the old and the new name. 
        /// The mapping can also be automatically retrieved from 
        /// existing ColumnMap attributes - in this case, leave it empty. 
        /// For arrays provide the array index and the new name. 
        /// </summary>
        public IEnumerable<ColumnMapping> ColumnMapping { get; set; }

        #endregion

        #region Constructors

        public ColumnRename()
        {
            RowTransformation = new RowTransformation<TInput, ExpandoObject>();
            TypeInfo = new ColumnRenameTypeInfo(typeof(TInput));
        }

        public ColumnRename(IEnumerable<ColumnMapping> columnMapping)
        {
            ColumnMapping = columnMapping;
        }

        #endregion

        #region Implement abstract methods

        protected override void InternalInitBufferObjects()
        {            
            InitRowTransformationManually();
            InitMappingDict();
            InitDefaultColumnMappingForPocos();
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        internal override void CompleteBufferOnPredecessorCompletion() => RowTransformation.CompleteBufferOnPredecessorCompletion();

        internal override void FaultBufferOnPredecessorCompletion(Exception e) => RowTransformation.FaultBufferOnPredecessorCompletion(e);

        public new IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
        {
            var errorSource = InternalLinkErrorTo(target);
            RowTransformation.ErrorSource = new ErrorSource() { Redirection = this.ErrorSource };
            return errorSource;
        }

        #endregion

        #region Implementation

        RowTransformation<TInput, ExpandoObject> RowTransformation;
        Dictionary<string, string> MappingDict = new Dictionary<string, string>();
        Dictionary<int, string> ArrayMappingDict = new Dictionary<int, string>();
        ColumnRenameTypeInfo TypeInfo;

        private void InitRowTransformationManually()
        {
            RowTransformation.TransformationFunc = RenameProperties;
            RowTransformation.CopyLogTaskProperties(this);
            RowTransformation.MaxBufferSize = this.MaxBufferSize;
            RowTransformation.InitBufferObjects();
        }

        private void InitMappingDict()
        {
            if (ColumnMapping == null) return;
            foreach (var map in ColumnMapping)
            {
                if (TypeInfo.IsArray)
                {
                    if (map.ArrayIndex == null || string.IsNullOrEmpty(map.NewName))
                        throw new ETLBoxException("When using arrays, ColumnMapping must provide a valid array index and new name!");
                    ArrayMappingDict.Add((int)map.ArrayIndex, map.NewName);
                }
                else
                {
                    if (string.IsNullOrEmpty(map.CurrentName) || string.IsNullOrEmpty(map.NewName))
                        throw new ETLBoxException("For objects (dynamic and POCOs), ColumnMapping must provide a valid current name and new name!");
                    MappingDict.Add(map.CurrentName, map.NewName);
                }
            }
        }

        private void InitDefaultColumnMappingForPocos()
        {
            if (ColumnMapping == null && !TypeInfo.IsArray && !TypeInfo.IsDynamic && TypeInfo.ColumnRenamingDict?.Count > 0)
                MappingDict = TypeInfo.ColumnRenamingDict;
        }


        private ExpandoObject RenameProperties(TInput row)
        {
            var result = new ExpandoObject();
            var resultAsDict = (IDictionary<string, object>)result;

            if (TypeInfo.IsArray)
                RenameArrayObject(row, resultAsDict);
            else if (TypeInfo.IsDynamic)
                RenameExpandObject(row, resultAsDict);
            else
                RenamePoco(row, resultAsDict);
            return result;
        }
              
        private void RenameArrayObject(TInput row, IDictionary<string, object> resultAsDict)
        {
            var ar = row as Array;
            for (int i = 0; i < ar.Length; i++)
            {
                if (!ArrayMappingDict.ContainsKey(i))
                    throw new ETLBoxException("When renaming arrays, provide a new name for every element in the array!");
                resultAsDict.Add(ArrayMappingDict[i], ar.GetValue(i));
            }
        }

        private void RenameExpandObject(TInput row, IDictionary<string, object> resultAsDict)
        {
            var inputAsDict = (IDictionary<string, object>)row;
            if (inputAsDict == null)
                throw new ETLBoxException($"Can't convert row into ExpandoObject: {row?.ToString() ?? ""}");
            foreach (var kvp in inputAsDict)
            {
                string newName = MappingDict.ContainsKey(kvp.Key) ? MappingDict[kvp.Key] : kvp.Key;
                resultAsDict.Add(newName, kvp.Value);
            }
        }

        private void RenamePoco(TInput row, IDictionary<string, object> resultAsDict)
        {
            for (int i = 0; i < TypeInfo.PropertyLength; i++)
            {
                string newName = MappingDict.ContainsKey(TypeInfo.Properties[i].Name) ? MappingDict[TypeInfo.Properties[i].Name] : TypeInfo.Properties[i].Name;
                resultAsDict.Add(newName, TypeInfo.Properties[i].GetValue(row));
            }
        }

        #endregion

    }

    /// <inheritdoc/>
    public class ColumnRename : ColumnRename<ExpandoObject>
    {
        public ColumnRename() : base()
        { }

        public ColumnRename(IEnumerable<ColumnMapping> columnMapping) : base(columnMapping)
        { }
    }
}
