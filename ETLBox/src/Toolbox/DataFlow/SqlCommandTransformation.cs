using System.Linq;
using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.ControlFlow;
using DotLiquid;

namespace ALE.ETLBox.src.Toolbox.DataFlow
{
    public abstract class SqlCommandTransformation<TInput, TOutput> : RowTransformation<TInput, TOutput>
    {
        public SqlCommandTransformation()
        {
            TransformationFunc = obj =>
            {
                var sql = TransformParameters(obj);

                var task = new SqlTask($"{nameof(SqlCommandTransformation)}->Execute", sql)
                {
                    ConnectionManager = ConnectionManager
                };
                var affectedRows = task.ExecuteNonQuery();

                return TransformResult(obj, affectedRows);
            };
        }

        public string SQLTemplate { get; set; }

        public virtual string TransformParameters(TInput input)
        {
            var templateSQL = Template.Parse(SQLTemplate);
            var inputDictionary = input is IDictionary<string, object> ? input as IDictionary<string, object> : input.GetType().GetProperties().ToDictionary(p => p.Name, p => p.GetValue(input));
            var resultQuery = templateSQL.Render(Hash.FromDictionary(inputDictionary));

            return resultQuery;
        }

        public abstract TOutput TransformResult(TInput obj, int affectedRows);
    }

    public class SqlCommandTransformation : SqlCommandTransformation<ExpandoObject, ExpandoObject>
    {
        public override string TransformParameters(ExpandoObject obj)
        {
            var templateSQL = Template.Parse(SQLTemplate);
            var resultQuery = templateSQL.Render(Hash.FromDictionary(obj));

            return resultQuery;
        }

        public override ExpandoObject TransformResult(ExpandoObject obj, int affectedRows)
        {
            return obj;
        }
    };
}
