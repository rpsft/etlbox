using ALE.ETLBox.Helper;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests
{
    public class SqlParserTests
    {
        [Fact]
        public void SqlSimple()
        {
            //Arrange
            List<string> expected = new List<string>() { "Col1", "Col2" };
            string sql = $@"SELECT Col1, Col2 FROM Table";

            //Act
            var actual = SqlParser.ParseColumnNames(sql);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void SqlOneColumn()
        {
            //Arrange
            List<string> expected = new List<string>() { "Col1" };
            string sql = $@"SELECT Col1";

            //Act
            var actual = SqlParser.ParseColumnNames(sql);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void SqlWithFunction()
        {
            //Arrange
            List<string> expected = new List<string>() { "Col1", "Col2" };
            string sql = $@"SELECT CASE WHEN ISNULL(Col1,'') IS NOT NULL THEN Col1 ELSE Col1 END AS Col1, 
Col2 
FROM Table";

            //Act
            var actual = SqlParser.ParseColumnNames(sql);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void SqlWithSeveralFunctions()
        {
            //Arrange
            List<string> expected = new List<string>() { "Col1", "Col2" };
            string sql = $@"SELECT CONCAT('','Test',ISNULL(Col1,''), GETDATE()) AS Col1, 
( CONVERT(INT, Col2) * 5) Col2 
FROM Table";

            //Act
            var actual = SqlParser.ParseColumnNames(sql);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void SqlSelectStart()
        {
            //Arrange
            List<string> expected = new List<string>() { };
            string sql = $@"SELECT * FROM Table";

            //Act
            var actual = SqlParser.ParseColumnNames(sql);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void SqlWithSchema()
        {
            //Arrange
            List<string> expected = new List<string>() { "Col1", "Col2" };
            string sql = $@"SELECT sou.Col1, sou.Col2 FROM table AS sou";

            //Act
            var actual = SqlParser.ParseColumnNames(sql);

            //Assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void SqlWithSchemaOneColumn()
        {
            //Arrange
            List<string> expected = new List<string>() { "Col1" };
            string sql = $@"SELECT sou.Col1 FROM table AS sou";

            //Act
            var actual = SqlParser.ParseColumnNames(sql);

            //Assert
            Assert.Equal(expected, actual);
        }


    }
}
