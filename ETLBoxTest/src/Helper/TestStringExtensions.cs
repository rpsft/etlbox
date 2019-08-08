using System;
using System.Collections.Generic;
using System.Text;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using ALE.ETLBox.Helper;

namespace ALE.ETLBoxTest
{
	[TestClass]
	public class TestStringExtensions
	{
		[TestMethod]
		public void TestStringReplaceNotExist()
		{
			Assert.AreEqual("test", "test".ReplaceIgnoreCase("something", "replacement"));
		}

		[TestMethod]
		public void TestStringReplaceDiffCase()
		{
			Assert.AreEqual("test other thing", "test some thing".ReplaceIgnoreCase("SOME", "other"));
		}

		[TestMethod]
		public void TestStringReplaceExactLength()
		{
			Assert.AreEqual("test this", "test thing".ReplaceIgnoreCase("thing", "this"));
		}
	}
}
