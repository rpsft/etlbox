using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests.Independent
{
    [Collection("Generic ControlFlow")]
    public class SequenceTests
    {
        public SequenceTests()
        { }

        bool Action1Executed { get; set; }

        void Action1()
        {
            Action1Executed = true;
        }

        [Fact]
        public void SimpleSequence()
        {
            //Arrange
            Action1Executed = false;
            //Act
            Sequence.Execute("Test sequence 1", Action1);
            //Assert
            Assert.True(Action1Executed);
        }

        bool Action2Executed { get; set; }

        void Action2(object parent)
        {
            Action2Executed = true;
            Assert.Equal("Test", parent);
        }

        [Fact]
        public void SequenceWithParent()
        {
            //Arrange
            Action2Executed = false;
            string test = "Test";
            //Act
            Sequence<object>.Execute("Test sequence 2", Action2, test);
            //Assert
            Assert.True(Action2Executed);
        }
    }
}
