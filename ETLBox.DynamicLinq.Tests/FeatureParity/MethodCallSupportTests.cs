using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Dynamic.Core;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.DynamicLinq;
using ALE.ETLBox.Scripting;
using ETLBox.Primitives;
using Xunit;

namespace ETLBox.DynamicLinq.Tests.FeatureParity;

/// <summary>
/// Feature-parity tests for the two expression engines used by ETLBox.Scripting:
/// Roslyn (via <see cref="ScriptBuilder"/>) and System.Linq.Dynamic.Core (via
/// <c>AsQueryable().Where/Any</c>).
///
/// These are not benchmarks - they document Yes/No on specific feature categories
/// raised by the reviewer (note 84243): "Вызов методов кажется супер важным,
/// без него мы не сможем работать со сложными типами, элементарно JsonNode в
/// строку не сможем преобразовать."
///
/// The matrix produced by these tests feeds the report and the
/// TECH-DEBT-Expression-Engine-Unification.md analysis section.
/// </summary>
public class MethodCallSupportTests
{
    private static readonly ParsingConfig s_parsingConfig =
        new() { ConvertObjectToSupportComparison = true };

    // ---- Built-in instance method on a string property ---------------------

    [Fact]
    public void Roslyn_StringLength_Works()
    {
        var row = MakeRow(("Type", (object)"Day"));
        var dict = (IDictionary<string, object?>)row;

        var runner = ScriptBuilder.Default.ForType(dict).CreateRunner<bool>("Type.Length > 0");

        var result = runner.RunAsync(dict).Result.ReturnValue;
        Assert.True(result);
    }

    [Fact]
    public void DynamicLinq_StringLength_Works()
    {
        var row = MakeRow(("Type", (object)"Day"));
        var (type, instance) = ExpandoTypeMapper.Map(row);
        var array = Array.CreateInstance(type, 1);
        array.SetValue(instance, 0);

        var passed = array.AsQueryable().Any(s_parsingConfig, "Type.Length > 0");
        Assert.True(passed);
    }

    // ---- Built-in instance method on DateTime ------------------------------

    [Fact]
    public void Roslyn_DateTimeAddDays_Works()
    {
        var date = new DateTime(2026, 1, 1);
        var row = MakeRow(("Date", (object)date));
        var dict = (IDictionary<string, object?>)row;

        var runner = ScriptBuilder
            .Default.ForType(dict)
            .CreateRunner<bool>("Date.AddDays(1).Year == 2026");

        var result = runner.RunAsync(dict).Result.ReturnValue;
        Assert.True(result);
    }

    [Fact]
    public void DynamicLinq_DateTimeAddDays_Works()
    {
        var date = new DateTime(2026, 1, 1);
        var row = MakeRow(("Date", (object)date));
        var (type, instance) = ExpandoTypeMapper.Map(row);
        var array = Array.CreateInstance(type, 1);
        array.SetValue(instance, 0);

        var passed = array.AsQueryable().Any(s_parsingConfig, "Date.AddDays(1).Year == 2026");
        Assert.True(passed);
    }

    // ---- Method call on user type (the JsonNode-style case in note 84243) --

    [Fact]
    public void Roslyn_UserTypeMethod_Works()
    {
        // SimpleBox is a tiny stand-in for any user type with a ToText() method.
        // Roslyn just compiles and dispatches - works out of the box.
        var row = MakeRow(("Box", (object)new SimpleBox(42)));
        var dict = (IDictionary<string, object?>)row;

        var runner = ScriptBuilder
            .Default.ForType(dict)
            .CreateRunner<bool>("Box.ToText() == \"box(42)\"");

        var result = runner.RunAsync(dict).Result.ReturnValue;
        Assert.True(result);
    }

    [Fact]
    public void DynamicLinq_UserTypeMethod_DoesNotWork_WithoutCustomTypes()
    {
        // Without registered custom types, calling a user type method throws.
        // "User type" here = type defined outside System / framework BCL; Dynamic LINQ
        // recognizes BCL types out of the box (string, DateTime, int, byte[]), but not
        // user-defined classes. SimpleBox stands in for any such type.
        var filtration = new ExpressionRowFiltration<RowWithBox>("Box.ToText() == \"box(42)\"");
        var rows = new[] { new RowWithBox { Box = new SimpleBox(42) } };
        var source = new MemorySource<RowWithBox>();
        foreach (var row in rows)
            source.DataAsList.Add(row);
        var dest = new MemoryDestination<RowWithBox>();
        var errorDest = new MemoryDestination<ETLBoxError>();
        source.LinkTo(filtration);
        filtration.LinkTo(dest);
        filtration.LinkErrorTo(errorDest);
        source.Execute();
        dest.Wait();
        errorDest.Wait();

        // Without RegisterCustomTypes, the parse fails per row, error goes to error buffer.
        Assert.Empty(dest.Data);
        Assert.Single(errorDest.Data);
    }

    [Fact]
    public void DynamicLinq_UserTypeMethod_Works_AfterRegisterCustomTypes()
    {
        // After RegisterCustomTypes(typeof(SimpleBox)), the user type is visible to
        // Dynamic LINQ's parser and its instance methods become callable from the
        // expression text. This closes the feature gap raised in note 84400.
        var filtration = new ExpressionRowFiltration<RowWithBox>("Box.ToText() == \"box(42)\"");
        filtration.RegisterCustomTypes(typeof(SimpleBox));
        var rows = new[]
        {
            new RowWithBox { Box = new SimpleBox(42) }, // passes
            new RowWithBox { Box = new SimpleBox(99) }, // dropped
        };
        var source = new MemorySource<RowWithBox>();
        foreach (var row in rows)
            source.DataAsList.Add(row);
        var dest = new MemoryDestination<RowWithBox>();
        source.LinkTo(filtration);
        filtration.LinkTo(dest);
        source.Execute();
        dest.Wait();

        Assert.Single(dest.Data);
        Assert.Equal(42, dest.Data.First().Box!.Value);
    }

    private sealed class RowWithBox
    {
        public SimpleBox? Box { get; set; }
    }

    // ---- Static method call ------------------------------------------------

    [Fact]
    public void Roslyn_StaticFormat_Works()
    {
        var row = MakeRow(("Type", (object)"Day"));
        var dict = (IDictionary<string, object?>)row;

        var runner = ScriptBuilder
            .Default.ForType(dict)
            .CreateRunner<bool>("string.Format(\"{0}\", Type) == \"Day\"");

        var result = runner.RunAsync(dict).Result.ReturnValue;
        Assert.True(result);
    }

    [Fact]
    public void DynamicLinq_StaticFormat_Works()
    {
        // Dynamic LINQ supports static methods on built-in framework types out of
        // the box: string.Format, Math.Max, DateTime.Parse, etc. The capability gap
        // (see DynamicLinq_UserTypeMethod_DoesNotWork_OutOfBox above) is specifically
        // for *user* types - those need ParsingConfig.CustomTypeProvider registration.
        var row = MakeRow(("Type", (object)"Day"));
        var (type, instance) = ExpandoTypeMapper.Map(row);
        var array = Array.CreateInstance(type, 1);
        array.SetValue(instance, 0);

        var passed = array
            .AsQueryable()
            .Any(s_parsingConfig, "string.Format(\"{0}\", Type) == \"Day\"");
        Assert.True(passed);
    }

    // ---- Helpers -----------------------------------------------------------

    private static ExpandoObject MakeRow(params (string key, object value)[] fields)
    {
        var row = new ExpandoObject();
        var dict = (IDictionary<string, object?>)row;
        foreach (var (key, value) in fields)
            dict[key] = value;
        return row;
    }

    public sealed class SimpleBox
    {
        public int Value { get; }

        public SimpleBox(int value) => Value = value;

        public string ToText() => $"box({Value})";
    }
}
