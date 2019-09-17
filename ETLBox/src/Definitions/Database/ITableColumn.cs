namespace ALE.ETLBox {
    public interface ITableColumn {
        string Name { get; }
        string DataType { get; }
        bool AllowNulls { get; }
        bool IsIdentity { get; }
        int? IdentitySeed { get; }
        int? IdentityIncrement { get; }
        bool IsPrimaryKey { get; }
        string DefaultValue { get; }
        string Collation { get; }
        string ComputedColumn { get; }
        bool HasComputedColumn { get; }

    }
}
