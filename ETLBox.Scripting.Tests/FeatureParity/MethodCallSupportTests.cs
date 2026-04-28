using System;
using System.Dynamic;
using System.Linq;
using System.Linq.Dynamic.Core;
using ALE.ETLBox.Scripting;
using Xunit;

namespace ETLBox.Scripting.Tests.FeatureParity;

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
    public void DynamicLinq_UserTypeMethod_DoesNotWork_OutOfBox()
    {
        // Without ParsingConfig.CustomTypeProvider, calling a user type method
        // throws ParseException. Documents the reviewer's concern that Dynamic
        // LINQ needs explicit registration.
        var row = MakeRow(("Box", (object)new SimpleBox(42)));
        var (type, instance) = ExpandoTypeMapper.Map(row);
        var array = Array.CreateInstance(type, 1);
        array.SetValue(instance, 0);

        Assert.ThrowsAny<Exception>(
            () => array.AsQueryable().Any(s_parsingConfig, "Box.ToText() == \"box(42)\"")
        );
    }

    // Note on escape hatch: Dynamic LINQ exposes IDynamicLinqCustomTypeProvider on
    // ParsingConfig.CustomTypeProvider. Registering a user type there makes its
    // instance methods callable from expression text - this is the answer to the
    // reviewer's "JsonNode -> string" objection (note 84243). The mechanism exists,
    // it just requires explicit setup per type. Concrete usage example belongs in
    // docs/dataflow/row-filtration.md if we decide to document the path; not asserted
    // here because the construction shape varies between Dynamic LINQ versions.

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
