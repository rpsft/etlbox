using ETLBox.Logging;
using Xunit;

namespace ETLBoxTests.ControlFlowTests
{
    [Collection("Generic ControlFlow")]
    public class CustomTaskTests
    {
        public CustomTaskTests()
        { }

        bool Action1Executed { get; set; }
        void Action1()
        {
            Action1Executed = true;
        }

        [Fact]
        public void SimpleAction()
        {
            //Arrange
            Action1Executed = false;
            //Act
            CustomTask.Execute("Test custom task 1", Action1);
            //Assert
            Assert.True(Action1Executed);
        }

        int Action2Value { get; set; }
        void Action2(int param1)
        {
            Action2Value = param1;
        }

        [Fact]
        public void ActionChangingValue()
        {
            //Arrange
            Action2Value = 0;
            //Act
            CustomTask.Execute("Test custom task 2", Action2, 5);
            //Assert
            Assert.Equal(5, Action2Value);
        }

        string Action3Value1 { get; set; }
        bool Action3Value2 { get; set; }
        void Action3(string param1, bool param2)
        {
            Action3Value1 = param1;
            Action3Value2 = param2;
        }

        [Fact]
        public void ActionWith2Parameter()
        {
            //Arrange
            Action3Value1 = null;
            Action3Value2 = false;
            //Act
            CustomTask.Execute("Test custom task 3", Action3, "t", true);
            //Assert
            Assert.Equal("t", Action3Value1);
            Assert.True(Action3Value2);
        }
    }
}
