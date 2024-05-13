using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using ALE.ETLBox.Common.DataFlow;
using JetBrains.Annotations;
using Microsoft.CSharp.RuntimeBinder;

namespace ALE.ETLBox.Scripting;

/// <inheritdoc />
[PublicAPI]
public class ScriptedTransformation : ScriptedRowTransformation<ExpandoObject, ExpandoObject> { }

/// <summary>
/// Transforms a row with a C# script expressions for each field.
/// </summary>
/// <typeparam name="TInput"></typeparam>
/// <typeparam name="TOutput"></typeparam>
[PublicAPI]
public class ScriptedRowTransformation<TInput, TOutput> : RowTransformation<TInput, TOutput>
{
    /// <summary>
    /// Mapping of input fields to output fields, where
    /// each key is the output field name, each value is the script to transform the input field to the output field
    /// </summary>
    public Dictionary<string, string> Mappings { get; set; } = new();

    /// <summary>
    /// Additional assembly FullName string to load for the script
    /// </summary>
    public IEnumerable<string> AdditionalAssemblyNames
    {
        get => _additionalAssemblies.Select(x => x.GetName().FullName);
        set => _additionalAssemblies = value
            .Select(Assembly.Load);
    }

    /// <summary>
    /// Indicates if transformation should fail when missing mapping field on source.
    /// * True: Transformation will fail when a field is missing in the source or script fails to compile.
    /// * False: Transformation will return null when a field is missing in the source, or script is failed to compile.
    /// </summary>
    public bool FailOnMissingField { get; set; } = true;

    private IEnumerable<Assembly> _additionalAssemblies = Array.Empty<Assembly>();
    private readonly ConcurrentDictionary<string, ScriptRunner<object>?> _runnersCache = new();

    public ScriptedRowTransformation()
    {
        if (typeof(TInput).IsArray || typeof(TOutput).IsArray)
            throw new ArgumentException(
                "Array types are not supported. Use a singular object type instead."
            );
        TransformationFunc = ScriptedTransformation;
    }

    [SuppressMessage("Critical Bug", "S4275:Getters and setters should access the expected fields")]
    public sealed override Func<TInput, TOutput> TransformationFunc
    {
        // This property needs to get sealed as it is called from constructor
        get => base.TransformationFunc;
        set => base.TransformationFunc = value;
    }

    private TOutput ScriptedTransformation(TInput arg)
    {
        return typeof(IDictionary<string, object?>).IsAssignableFrom(typeof(TInput))
            ? (TOutput)TransformWithScriptDynamic((IDictionary<string, object?>)arg!)
            : TransformWithScriptTyped(arg);
    }

    private dynamic TransformWithScriptDynamic(IDictionary<string, object?> arg)
    {
        dynamic output =
            Activator.CreateInstance(typeof(TOutput))
            ?? throw new InvalidOperationException(
                $"Could not create instance of output type '{typeof(TOutput).FullName}'. This may be caused by a missing parameterless constructor."
            );
        var type = ScriptBuilder.Default.ForType(arg).WithReferences(_additionalAssemblies);

        foreach (var key in Mappings.Keys)
        {
            var runner = GetScriptRunner(key, type);
            dynamic? value = runner?.RunAsync(arg).Result.ReturnValue;
            try
            {
                ((IDictionary<string, object?>)output)[key] = value;
            }
            catch (RuntimeBinderException e)
            {
                throw new ArgumentException(
                    $"Property {key} not found on type {typeof(TOutput).FullName}.",
                    e
                );
            }
            catch (Exception e)
            {
                throw new ArgumentException(
                    $"Could not set property {key} on type {typeof(TOutput).FullName}.",
                    e
                );
            }
        }

        return output;
    }

    private TOutput TransformWithScriptTyped(TInput arg)
    {
        TOutput output =
            (TOutput)Activator.CreateInstance(typeof(TOutput))!
            ?? throw new InvalidOperationException(
                $"Could not create instance of output type {typeof(TOutput).FullName}. This may be caused by a missing parameterless constructor."
            );
        var builder = ScriptBuilder.Default.ForType<TInput>();
        foreach (var key in Mappings.Keys)
        {
            var runner = GetScriptRunner(key, builder);

            var value = runner?.RunAsync(arg).Result.ReturnValue;
            try
            {
                var property = typeof(TOutput).GetProperty(key);
                if (property == null)
                    throw new ArgumentException(
                        $"Property {key} not found on type {typeof(TOutput).FullName}."
                    );
                property.SetValue(output, value);
            }
            catch (Exception e)
            {
                throw new ArgumentException(
                    $"Could not set property {key} on type {typeof(TOutput).FullName}.",
                    e
                );
            }
        }

        return output;
    }

    private ScriptRunner<object>? GetScriptRunner(string key, TypedScriptBuilder builder) =>
        _runnersCache.GetOrAdd(
            Mappings[key],
            script =>
            {
                var runner = builder.CreateRunner<object>(script);
                var diagnostics = runner.Script.Compile();

                if (!diagnostics.Any())
                {
                    return runner;
                }

                if (FailOnMissingField)
                {
                    throw new ArgumentException(
                        $"Could not compile script for '{typeof(TOutput).FullName}.{key}' => {Mappings[key]}.",
                        diagnostics.First().GetMessage()
                    );
                }

                return null;
            }
        );
}
