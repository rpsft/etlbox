using System;
using System.Collections.Generic;
using System.Linq;
using TSQL;
using TSQL.Statements;

namespace ALE.ETLBox.Helper
{
    public static class SqlParser
    {
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
