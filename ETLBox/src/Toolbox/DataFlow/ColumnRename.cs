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
        public IEnumerable<ColumnMap> ColumnMapping { get; set; }

        public IEnumerable<string> RemoveColumns { get; set; }

        #endregion

        #region Constructors

        public ColumnRename()
        {
            RowTransformation = new RowTransformation<TInput, ExpandoObject>();
            TypeInfo = new ColumnRenameTypeInfo(typeof(TInput));
        }

        public ColumnRename(IEnumerable<ColumnMap> columnMap)
        {
            ColumnMapping = columnMap;
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

        internal override void CompleteBuffer() => RowTransformation.CompleteBuffer();

        internal override void FaultBuffer(Exception e) => RowTransformation.FaultBuffer(e);

        public new IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
        {
            var errorSource = InternalLinkErrorTo(target);
            RowTransformation.ErrorSource = new ErrorSource() { Redirection = this.ErrorSource };
            return errorSource;
        }

        #endregion

        #region Implementation

        RowTransformation<TInput, ExpandoObject> RowTransformation;
        Dictionary<string, ColumnMap> MappingDict = new Dictionary<string, ColumnMap>();
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
                    if (map.ArrayIndex == null || (string.IsNullOrEmpty(map.NewName)
                        && map.RemoveColumn == false))
                        throw new ETLBoxException("When using arrays, ColumnMapping must provide a valid array index and new name or RemoveColumn set to true!");
                    MappingDict.Add(map.ArrayIndex.ToString(), map);
                }
                else
                {
                    if (string.IsNullOrEmpty(map.CurrentName) || (string.IsNullOrEmpty(map.NewName)
                        && map.RemoveColumn == false))
                        throw new ETLBoxException("For objects (dynamic and POCOs), ColumnMapping must provide a valid current name and either a new name or RemoveColumn set to true!");
                    MappingDict.Add(map.CurrentName, map);
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
                RenameExpandoObject(row, resultAsDict);
            else
                RenamePoco(row, resultAsDict);
            return result;
        }

        private void RenameArrayObject(TInput row, IDictionary<string, object> resultAsDict)
        {
            var ar = row as Array;
            for (int i = 0; i < ar.Length; i++)
            {
                if (!MappingDict.ContainsKey(i.ToString()))
                    throw new ETLBoxException("When renaming arrays, provide a new name for every element in the array!");
                ColumnMap cm = MappingDict[i.ToString()];
                if (cm == null || cm.RemoveColumn == false)
                    resultAsDict.Add(cm.NewName, ar.GetValue(i));
            }
        }

        private void RenameExpandoObject(TInput row, IDictionary<string, object> resultAsDict)
        {
            var inputAsDict = (IDictionary<string, object>)row;
            if (inputAsDict == null)
                throw new ETLBoxException($"Can't convert row into ExpandoObject: {row?.ToString() ?? ""}");
            foreach (var kvp in inputAsDict)
            {
                ColumnMap cm = MappingDict.ContainsKey(kvp.Key) ? MappingDict[kvp.Key] : null;
                if (cm == null || cm?.RemoveColumn == false)
                    resultAsDict.Add(cm?.NewName ?? kvp.Key, kvp.Value);
            }
        }

        private void RenamePoco(TInput row, IDictionary<string, object> resultAsDict)
        {
            for (int i = 0; i < TypeInfo.PropertyLength; i++)
            {
                ColumnMap cm = MappingDict.ContainsKey(TypeInfo.Properties[i].Name) ? MappingDict[TypeInfo.Properties[i].Name] : null;
                if (cm == null || cm?.RemoveColumn == false)
                    resultAsDict.Add(cm?.NewName ?? TypeInfo.Properties[i].Name, TypeInfo.Properties[i].GetValue(row));
            }
        }

        #endregion

    }

    /// <inheritdoc/>
    public class ColumnRename : ColumnRename<ExpandoObject>
    {
        public ColumnRename() : base()
        { }

        public ColumnRename(IEnumerable<ColumnMap> columnMap) : base(columnMap)
        { }
    }
}
