using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests
{
    public class StringExtensionTests
    {
        [Fact]
        public void TestStringReplaceNotExist()
        {
            Assert.Equal("test", "test".ReplaceIgnoreCase("something", "replacement"));
        }

        [Fact]
        public void TestStringReplaceDiffCase()
        {
            Assert.Equal("test other thing", "test some thing".ReplaceIgnoreCase("SOME", "other"));
        }

        [Fact]
        public void TestStringReplaceExactLength()
        {
            Assert.Equal("test this", "test thing".ReplaceIgnoreCase("thing", "this"));
        }
    }
}
