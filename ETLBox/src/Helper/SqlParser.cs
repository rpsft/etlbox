using System.Collections.Generic;
using System.Linq;
using TSQL;
using TSQL.Statements;

namespace ETLBox.Helper
{
    /// <summary>
    /// Helper class for parsing sql statements
    /// </summary>
    public static class SqlParser
    {
        /// <summary>
        /// This method attempts to parse column names from any sql statement.
        /// E.g. SELECT 1 AS 'Test', Col2, t2.Col3 FROM table1 t1 INNER JOIN t2 ON t1.Id = t2.Id
        /// will return Test, Col2 and Col3 als column names.
        /// </summary>
        /// <param name="sql">The sql code from which the column names should be parsed</param>
        /// <returns>The names of the columns in the sql</returns>
        public static List<string> ParseColumnNames(string sql)
        {

            var statement = TSQLStatementReader.ParseStatements(sql).FirstOrDefault() as TSQLSelectStatement;

            List<string> result = new List<string>();
            int functionStartCount = 0;
            string prevToken = string.Empty;
            foreach (var token in statement.Select.Tokens)
            {
                if (token.Type == TSQL.Tokens.TSQLTokenType.Character &&
                    token.Text == "(")
                    functionStartCount++;
                else if (token.Type == TSQL.Tokens.TSQLTokenType.Character &&
                    token.Text == ")")
                    functionStartCount--;
                if (token.Type == TSQL.Tokens.TSQLTokenType.Identifier)
                    prevToken = token.Text;
                if (token.Type == TSQL.Tokens.TSQLTokenType.Character &&
                    functionStartCount <= 0 &&
                    token.Text == ","
                    )
                    result.Add(prevToken);
            }
            if (prevToken != string.Empty)
                result.Add(prevToken);
            return result;
        }
    }
}
