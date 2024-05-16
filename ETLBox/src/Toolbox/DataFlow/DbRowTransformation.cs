using System.Linq;
using ALE.ETLBox.Common;
using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.ControlFlow;
using ETLBox.Primitives;
using TypeInfo = ALE.ETLBox.Common.DataFlow.TypeInfo;

namespace ALE.ETLBox.DataFlow;

[PublicAPI]
public class DbRowTransformation<TInput> : RowTransformation<TInput>
{
    /* Public properties */
    /// <summary>
    /// If you don't want ETLBox to dynamically read the destination table definition from the database,
    /// you can provide your own table definition.
    /// </summary>
    public TableDefinition DestinationTableDefinition { get; set; }

    /// <summary>
    /// Name of the target table that receives the data from the data flow.
    /// </summary>
    public string TableName { get; set; }

    /* Private stuff */
    private TypeInfo TypeInfo { get; set; }
    private bool HasDestinationTableDefinition => DestinationTableDefinition != null;
    private bool HasTableName => !string.IsNullOrWhiteSpace(TableName);
    private TableData<TInput> TableData { get; set; }
    private IConnectionManager BulkInsertConnectionManager { get; set; }

    public DbRowTransformation()
    {
        InitObjects();
        TransformationFunc = source =>
        {
            PrepareWrite();
            return TryBulkInsertData(source) ? source : default;
        };
    }

    public DbRowTransformation(string tableName)
        : this()
    {
        TableName = tableName;
        InitObjects();
    }

    public DbRowTransformation(IConnectionManager connectionManager, string tableName)
        : this(tableName)
    {
        ConnectionManager = connectionManager;
    }

    public sealed override Func<TInput, TInput> TransformationFunc
    {
#pragma warning disable S4275 // we are only sealing the property to prevent it from being overridden
        get => base.TransformationFunc;
        set => base.TransformationFunc = value;
#pragma warning restore S4275
    }

    private void InitObjects()
    {
        TypeInfo = new TypeInfo(typeof(TInput)).GatherTypeInfo();
    }

    private void LoadTableDefinitionFromTableName()
    {
        if (HasTableName)
            DestinationTableDefinition = TableDefinition.GetDefinitionFromTableName(
                DbConnectionManager,
                TableName
            );
        else if (!HasDestinationTableDefinition && !HasTableName)
            throw new ETLBoxException(
                "No Table definition or table name found! You must provide a table name or a table definition."
            );
    }

    private void PrepareWrite()
    {
        if (!HasDestinationTableDefinition)
            LoadTableDefinitionFromTableName();
        BulkInsertConnectionManager = DbConnectionManager.CloneIfAllowed();
        BulkInsertConnectionManager.IsInBulkInsert = true;
        BulkInsertConnectionManager.PrepareBulkInsert(DestinationTableDefinition.Name);
        TableData = new TableData<TInput>(DestinationTableDefinition, 1);
    }

    private bool TryBulkInsertData(params TInput[] data)
    {
        TryAddDynamicColumnsToTableDef(data);
        try
        {
            TableData.ClearData();
            ConvertAndAddRows(data);
            var sql = new SqlTask(this, "Execute Bulk insert")
            {
                DisableLogging = true,
                ConnectionManager = BulkInsertConnectionManager
            };
            sql.BulkInsert(TableData, DestinationTableDefinition.Name);
            return true;
        }
        catch (Exception e)
        {
            if (!ErrorHandler.HasErrorBuffer)
                throw;
            ErrorHandler.Send(e, ErrorHandler.ConvertErrorData(data));
            return false;
        }
    }

    private void TryAddDynamicColumnsToTableDef(TInput[] data)
    {
        if (!TypeInfo.IsDynamic || data.Length <= 0)
        {
            return;
        }

        foreach (
            var dynamicObject in data.Select(x => x as IDictionary<string, object>)
                .Where(x => x != null)
        )
        {
            foreach (var key in dynamicObject.Select(c => c.Key))
            {
                var newPropIndex = TableData.DynamicColumnNames.Count;
                if (!TableData.DynamicColumnNames.ContainsKey(key))
                    TableData.DynamicColumnNames.Add(key, newPropIndex);
            }
        }
    }

    private void ConvertAndAddRows(TInput[] data)
    {
        foreach (var currentRow in data)
        {
            if (currentRow == null)
                continue;

            TableData.Rows.Add(
                TypeInfo.GetTypeInfoGroup() switch
                {
                    TypeInfo.TypeInfoGroup.Array => currentRow as object[],
                    TypeInfo.TypeInfoGroup.Dynamic
                        => ConvertDynamicRow(currentRow as IDictionary<string, object>),
                    _ => ConvertObjectRow(currentRow)
                }
            );
        }
    }

    private object[] ConvertObjectRow(TInput currentRow)
    {
        var rowResult = new object[TypeInfo.PropertyLength];
        var index = 0;
        foreach (PropertyInfo propInfo in TypeInfo.Properties)
        {
            rowResult[index] = propInfo.GetValue(currentRow);
            index++;
        }

        return rowResult;
    }

    private object[] ConvertDynamicRow(IDictionary<string, object> propertyValues)
    {
        var rowResult = new object[TableData.DynamicColumnNames.Count];
        foreach (var prop in propertyValues)
        {
            var columnIndex = TableData.DynamicColumnNames[prop.Key];
            rowResult[columnIndex] = prop.Value;
        }

        return rowResult;
    }
}
