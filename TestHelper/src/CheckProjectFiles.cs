using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests
{
    public class CheckProjectFiles
    {

        string noSymbolsPackageXml = $@"
<PropertyGroup Condition=""'$(Configuration)|$(Platform)'=='Release|AnyCPU'"">
    <DebugType>none</DebugType>
    <DebugSymbols>false</DebugSymbols>
</PropertyGroup>";

        [Fact]
        public void CheckIfReleasesHaveNoSymbols()
        {
            int found = 0;
            var dirs = Directory.GetDirectories("../../../../");
            foreach (var dir in dirs)
            {
                string cleanedDirName = dir.Replace("../", "").ToLower();
                if (cleanedDirName.StartsWith("etlbox") && cleanedDirName != "etlboxdocu")
                {
                    var projfile = Directory.GetFiles(dir, "*.csproj").First();
                    var cont = File.ReadAllText(projfile);
                    Assert.Contains(noSymbolsPackageXml.Replace(" ", ""), cont.Replace(" ", ""));
                    found++;
                }
            }

            Assert.True(found > 0);
        }
    }
}
