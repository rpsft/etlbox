using System.IO;
using System.Linq;
using System.Text;
using Xunit;

namespace TestHelper
{
    public class CheckProjectFiles
    {
        private readonly string noSymbolsPackageXml =
            @"
<PropertyGroup Condition=""'$(Configuration)|$(Platform)'=='Release|AnyCPU'"">
<DebugType>none</DebugType>
<DebugSymbols>false</DebugSymbols>
</PropertyGroup>";

        [Fact]
        public void CheckIfReleasesHaveNoSymbols()
        {
            var found = 0;
            var dirs = Directory.GetDirectories("../../../../");
            foreach (var dir in dirs)
            {
                var cleanedDirName = dir.Replace("../", "").ToLower();
                if (cleanedDirName.StartsWith("etlbox") && cleanedDirName != "etlboxdocu")
                {
                    var projectFile = Directory.GetFiles(dir, "*.csproj").First();
                    var cont = File.ReadAllText(projectFile);

                    Assert.Contains(NormalizeText(noSymbolsPackageXml), NormalizeText(cont));
                    found++;
                }
            }

            Assert.True(found > 0);
        }

        private string NormalizeText(string text) =>
            new StringBuilder(text).Replace(" ", "").Replace("\r", "").Replace("\n", "").ToString();
    }
}
