using System;
using System.Collections.Generic;
using System.Linq;
using ALE.ETLBox.Common.DataFlow;
using JetBrains.Annotations;
using Microsoft.CSharp.RuntimeBinder;

namespace ALE.ETLBox.Scripting;

public class ScriptedTransformation<TInput> : ScriptedRowTransformation<TInput, TInput> { }

[PublicAPI]
public class ScriptedRowTransformation<TInput, TOutput> : RowTransformation<TInput, TOutput>
{
    public Dictionary<string, string> Mappings { get; set; } = new Dictionary<string, string>();

    /// <summary>
    /// Indicates if transformation should fail when missing mapping field on source
    /// </summary>
    public bool FailOnMissingField { get; set; } = false;

    public ScriptedRowTransformation()
    {
        if (typeof(TInput).IsArray || typeof(TOutput).IsArray)
            throw new ArgumentException(
                "Array types are not supported. Use a singular object type instead."
            );
        TransformationFunc = ScriptedTransformation;
    }

    private TOutput ScriptedTransformation(TInput arg)
    {
        if (typeof(IDictionary<string, object?>).IsAssignableFrom(typeof(TInput)))
        {
            return TransformWithScriptDynamic((IDictionary<string, object?>)arg!);
        }
        else
        {
            return TransformWithScriptTyped(arg);
        }
    }

    private dynamic TransformWithScriptDynamic(IDictionary<string, object?> arg)
    {
        dynamic output =
            Activator.CreateInstance(typeof(TOutput))
            ?? throw new InvalidOperationException(
                $"Could not create instance of output type {typeof(TOutput).FullName}. This may be caused by a missing parameterless constructor."
            );
        var type = ScriptBuilder.Default.ForType(arg);

        foreach (var key in Mappings.Keys)
        {
            var runner = type.CreateRunner<object>(Mappings[key]);
            var diagnostics = runner.Script.Compile();

            dynamic value;
            if (diagnostics.Any())
            {
                if (FailOnMissingField)
                {
                    throw new ArgumentException(
                        $"Could not compile script for '{typeof(TOutput).FullName}.{key}' => {Mappings[key]}.",
                        diagnostics.First().GetMessage());
                }
                value = null!;
            }
            else
            {
                value = runner.RunAsync(arg).Result.ReturnValue;
            }
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
            (TOutput)Activator.CreateInstance(typeof(TOutput))
            ?? throw new InvalidOperationException(
                $"Could not create instance of output type {typeof(TOutput).FullName}. This may be caused by a missing parameterless constructor."
            );
        var builder = ScriptBuilder.Default.ForType<TInput>();
        foreach (var key in Mappings.Keys)
        {
            var runner = builder.CreateRunner(Mappings[key]);
            var diagnostics = runner.Script.Compile();
            if (diagnostics.Any())
            {
                throw new ArgumentException(
                    $"Could not compile script for property {key} on type {typeof(TOutput).FullName}.",
                    diagnostics.First().GetMessage()
                );
            }

            object value = runner.RunAsync(arg).Result.ReturnValue;
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
}
