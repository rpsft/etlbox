using ALE.ETLBox.Common.DataFlow;
using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;

namespace ALE.ETLBox.Json.Extensions;

/// <summary>
/// Extension methods for registering ETLBox.Json components with <see cref="IServiceCollection"/>.
/// </summary>
[PublicAPI]
public static class EtlBoxJsonServiceCollectionExtensions
{
    /// <summary>
    /// Registers ETLBox.Json data flow components as transient services.
    /// </summary>
    public static IServiceCollection AddEtlBoxJson(this IServiceCollection services)
    {
        services.AddTransient<JsonTransformation>();
        return services;
    }
}
