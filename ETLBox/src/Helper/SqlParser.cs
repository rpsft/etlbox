using System.Linq;
using TSQL;
using TSQL.Statements;
using TSQL.Tokens;

namespace ALE.ETLBox.Helper
{
    [PublicAPI]
    public static class SqlParser
    {
        public static List<string> ParseColumnNames(string sql)
        {
            List<string> result = new List<string>();
            if (
                TSQLStatementReader.ParseStatements(sql).FirstOrDefault()
                is not TSQLSelectStatement statement
            )
                return result;

            int bracesNestingLevel = 0;
            string prevToken = string.Empty;
            foreach (var token in statement.Select.Tokens)
            {
                CheckOpeningAndClosingBraces(token, ref bracesNestingLevel);

                if (token.Type == TSQLTokenType.Identifier)
                    prevToken = token.Text;
                if (
                    token.Type == TSQLTokenType.Character
                    && bracesNestingLevel <= 0
                    && token.Text == ","
                )
                    result.Add(prevToken);
            }
            if (prevToken != string.Empty)
                result.Add(prevToken);
            return result;
        }

        private static void CheckOpeningAndClosingBraces(TSQLToken token, ref int bracesNesting)
        {
            if (token.Type == TSQLTokenType.Character && token.Text == "(")
                bracesNesting++;
            else if (token.Type == TSQLTokenType.Character && token.Text == ")")
                bracesNesting--;
        }
    }
}
