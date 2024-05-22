using ALE.ETLBox.DataFlow;

namespace ALE.ETLBox.src.Toolbox.DataFlow
{
    public class SqlQueryTransformation<TInput, TOutput>: RowMultiplication<TInput, TOutput>
    {
        public SqlQueryTransformation() 
        { 
            MultiplicationFunc = obj => 
            { 
                var dbSource = new DbSource<TOutput>(ConnectionManager);
                var sql = TransformParameters(obj);
                dbSource.Sql = sql;
                var dest = new MemoryDestination<TOutput>();
                dbSource.LinkTo(dest);
                dbSource.Execute();
                dest.Wait();
                return dest.Data;
            };
        }

        public string SQL { get;set; }

        public virtual string TransformParameters(TInput obj)
        { 
            return SQL;
        }
    }

    public class SqlQueryTransformation: SqlQueryTransformation<ExpandoObject, ExpandoObject>
    {
        public override string TransformParameters(ExpandoObject obj)
        {
            return SQL.Replace("{{lastId}}", (obj as IDictionary<string, object>)["LastId"]?.ToString());
        }
    };
}
