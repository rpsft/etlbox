using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;

namespace ALE.ETLBox.Scripting.Extensions;

/// <summary>
/// Extension methods for registering ETLBox.Scripting components with <see cref="IServiceCollection"/>.
/// </summary>
[PublicAPI]
public static class EtlBoxScriptingServiceCollectionExtensions
{
    /// <summary>
    /// Registers ETLBox.Scripting data flow components as transient services.
    /// </summary>
    public static IServiceCollection AddEtlBoxScripting(this IServiceCollection services)
    {
        services.AddTransient<ScriptedTransformation>();
        return services;
    }
}
