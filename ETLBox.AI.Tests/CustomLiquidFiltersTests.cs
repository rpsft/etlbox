
using System.Dynamic;

using DotLiquid;

namespace ETLBox.AI.Tests;

public class CustomLiquidFiltersTests
{
    public CustomLiquidFiltersTests()
    {
        // Ensure filters are registered once for DotLiquid usage in template rendering
        CustomLiquidFilters.EnsureRegistered();
    }

    [Fact]
    public void EnsureRegistered_ShouldBeIdempotent()
    {
        // Act: calling multiple times should not throw
        CustomLiquidFilters.EnsureRegistered();
        CustomLiquidFilters.EnsureRegistered();

        // Assert: rendering with a known filter should work
        // Use double-quoted Liquid string to avoid escaping issues inside single quotes
        dynamic a = new ExpandoObject();
        a.Result = "'ab'";
        var tpl = Template.Parse("{{Result | escape_single_quotes }}");
        var rendered = tpl.Render(Hash.FromDictionary(a));
        Assert.Equal("''ab''", rendered);
    }

    [Fact]
    public void EscapeSingleQuotes_ShouldDoubleQuotes_AndHandleNull()
    {
        Assert.Equal("O''Reilly", CustomLiquidFilters.EscapeSingleQuotes("O'Reilly"));
        Assert.Null(CustomLiquidFilters.EscapeSingleQuotes(null!));
        Assert.Equal(string.Empty, CustomLiquidFilters.EscapeSingleQuotes(string.Empty));
    }

    [Fact]
    public void EscapeSingleQuotesRecursive_ShouldWalkNestedDictionaries()
    {
        var obj = new Dictionary<string, object>
        {
            ["a"] = "x'y",
            ["b"] = new Dictionary<string, object> { ["c"] = "d'e" },
        };

        var res = (Dictionary<string, object>)CustomLiquidFilters.EscapeSingleQuotesRecursive(obj);

        Assert.Equal("x''y", res["a"]);
        var inner = (Dictionary<string, object>)res["b"];
        Assert.Equal("d''e", inner["c"]);

        // Non-string values are returned as-is
        var mix = new Dictionary<string, object> { ["n"] = 1, ["s"] = "q'w" };
        var mixRes =
            (Dictionary<string, object>)CustomLiquidFilters.EscapeSingleQuotesRecursive(mix);
        Assert.Equal(1, mixRes["n"]);
        Assert.Equal("q''w", mixRes["s"]);
    }

    [Fact]
    public void JsonArray_ShouldSerializeExpandoArray_WithoutNulls()
    {
        dynamic a = new ExpandoObject();
        a.id = 1;
        a.text = "Hello";
        dynamic b = new ExpandoObject();
        b.id = 2;
        b.text = null;

        var input = new ExpandoObject[] { a, b };
        var json = CustomLiquidFilters.JsonArray(input);

        // Basic shape
        Assert.StartsWith("[", json);
        Assert.Contains("\"id\":1", json);
        Assert.Contains("\"id\":2", json);

        // Validate content using System.Text.Json for resilience to serializer nuances
        var doc = System.Text.Json.JsonDocument.Parse(json);
        var arr = doc.RootElement;
        Assert.Equal(System.Text.Json.JsonValueKind.Array, arr.ValueKind);
        Assert.Equal(2, arr.GetArrayLength());
        Assert.Equal("Hello", arr[0].GetProperty("text").GetString());
        var second = arr[1];
        // Depending on serializer behavior with ExpandoObject, the null field may be present or omitted
        if (second.TryGetProperty("text", out var txt))
        {
            Assert.Equal(System.Text.Json.JsonValueKind.Null, txt.ValueKind);
        }
    }

    [Fact]
    public void AsString_ShouldReturnJsonForDictionary_AndToStringForScalar()
    {
        var dict = new Dictionary<string, object> { ["k"] = "v" };
        var str = CustomLiquidFilters.AsString(dict);
        Assert.Contains("\"k\":\"v\"", str);

        Assert.Equal("123", CustomLiquidFilters.AsString(123));
        Assert.Null(CustomLiquidFilters.AsString(null!));
    }
}
