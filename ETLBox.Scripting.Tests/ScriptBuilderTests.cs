using System.Dynamic;
using ALE.ETLBox.Scripting;
using JetBrains.Annotations;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;

namespace ETLBox.Scripting.Tests;

public class ScriptBuilderTests
{
    [Fact]
    public void ShouldCompileScriptDirectly()
    {
        // Arrange
        var options = ScriptOptions.Default.WithImports("System.Math");
        var script = CSharpScript
            .Create("int X = 1; int Y = 2;", options: options)
            .ContinueWith("X + Y", options);
        // Act
        var diagnostics = script.Compile();
        var result = script.RunAsync().Result.ReturnValue;
        // Assert
        Assert.Empty(diagnostics);
        Assert.Equal(3, result);
    }

    [Fact]
    public void ShouldCompileScriptWithDynamicGlobals()
    {
        // Arrange
        dynamic context = new ExpandoObject();
        context.X = 1;
        context.Y = 2;
        var builder = ScriptBuilder.Default.ForType(context);
        var runner = builder.CreateRunner("X + Y");
        // Act
        var diagnostics = runner.Script.Compile();
        var result = runner.RunAsync(context).Result.ReturnValue;
        // Assert
        Assert.Empty(diagnostics);
        Assert.Equal(3, result);
    }

    [Fact]
    public async Task ShouldExecuteWithComplexNestedObject()
    {
        var factory = new ScriptBuilder();
        string script =
            "(Global1.Number+WhatEverNameIWant.Number).ToString() + Global1.Text + WhatEverNameIWant.Text";

        dynamic globals = new ExpandoObject();
        globals.Global1 = new MyCoolClass() { Number = 100, Text = "Something" };
        globals.WhatEverNameIWant = new MyCoolClass() { Number = 500, Text = "Longer Text Value" };
        globals.Global2 = new ExpandoObject();
        globals.Global2.Number = 600;

        // Act
        var builder = factory.ForType(globals);
        var runner = builder.CreateRunner(script);
        var result = await runner.RunAsync(globals);

        // Assert
        Assert.Equal("600SomethingLonger Text Value", result.ReturnValue);
    }

    [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
    public class MyCoolClass
    {
        public string? Text { get; set; }
        public int Number { get; set; }
    }
}
