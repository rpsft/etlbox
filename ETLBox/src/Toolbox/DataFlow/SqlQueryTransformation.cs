using System.Linq;
using ALE.ETLBox.DataFlow;
using DotLiquid;

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

        public string SQLTemplate { get;set; }

        public virtual string TransformParameters(TInput input)
        { 
            var templateSQL = Template.Parse(SQLTemplate);
            var inputDictionary = input is IDictionary<string, object> ? input as IDictionary<string, object> : input.GetType().GetProperties().ToDictionary(p => p.Name, p => p.GetValue(input));
            var resultQuery = templateSQL.Render(Hash.FromDictionary(inputDictionary));

            return resultQuery;
        }
    }

    public class SqlQueryTransformation: SqlQueryTransformation<ExpandoObject, ExpandoObject>
    {
        public override string TransformParameters(ExpandoObject obj)
        {
            var templateSQL = Template.Parse(SQLTemplate);            
            var resultQuery = templateSQL.Render(Hash.FromDictionary(obj));

            return resultQuery;
        }
    };
}
