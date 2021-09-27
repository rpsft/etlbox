using System;
using System.Collections.Generic;

namespace ETLBox.ControlFlow
{
    public interface IConstraint
    {
        IList<string> ColumnNames { get; }

        string ConstraintName { get; }
    }

    public abstract class Constraint : IConstraint
    {      
        public IList<string> ColumnNames { get;set;}

        public string ConstraintName { get; set; }

        public Constraint() {

        }

        public Constraint(string columnName) {
            ColumnNames = new[] { columnName };
        }

        public Constraint(IList<string> columnNames) {
            ColumnNames = columnNames;
        }

        internal virtual void Validate() {
            if (ColumnNames == null || ColumnNames.Count == 0)
                throw new ArgumentException("A constraint needs at least one column name assigned!", nameof(ColumnNames));
        }
    }

    public sealed class PrimaryKeyConstraint : Constraint
    {
        public PrimaryKeyConstraint() : base() { }

        public PrimaryKeyConstraint(string columnName) : base(columnName) { }

        public PrimaryKeyConstraint(IList<string> columnNames) : base(columnNames) { }
    }

    public sealed class UniqueKeyConstraint: Constraint
    {
        public UniqueKeyConstraint() : base() { }

        public UniqueKeyConstraint(string columnName) : base(columnName) { }

        public UniqueKeyConstraint(IList<string> columnNames) : base(columnNames) { }
    }

    public sealed class ForeignKeyConstraint : Constraint
    {
        public ICollection<string> ReferenceColumnNames { get; set; }

        public string ReferenceTableName { get; set; }

        public bool OnDeleteCascade { get; set; }

        public ForeignKeyConstraint() : base() { }

        public ForeignKeyConstraint(string columnName, string referenceColumnName, string referenceTableName) 
            : base(columnName) {
            ReferenceColumnNames = new[] { referenceColumnName };
            ReferenceTableName = referenceTableName;
        }

        public ForeignKeyConstraint(IList<string> columnNames, IList<string> referenceColumnNames, string referenceTableName) 
            : base(columnNames) {
            ReferenceColumnNames = referenceColumnNames;
            ReferenceTableName = referenceTableName;
        }

        internal override void Validate() {
            base.Validate();
            if (ReferenceColumnNames == null || ReferenceColumnNames.Count == 0)
                throw new ArgumentException("A foreign key constraint needs at least one reference column name assigned!",nameof(ReferenceColumnNames));
            if (string.IsNullOrEmpty(ReferenceTableName))
                throw new ArgumentException("A foreign key constraint needs a reference table name assigned!",nameof(ReferenceTableName));
        }
    }
}