using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;

namespace ALE.ETLBox.DynamicLinq.Extensions;

/// <summary>
/// Extension methods for registering ETLBox.DynamicLinq components with <see cref="IServiceCollection"/>.
/// </summary>
[PublicAPI]
public static class EtlBoxDynamicLinqServiceCollectionExtensions
{
    /// <summary>
    /// Registers ETLBox.DynamicLinq data flow components as transient services.
    /// </summary>
    public static IServiceCollection AddEtlBoxDynamicLinq(this IServiceCollection services)
    {
        services.AddTransient<ExpressionRowFiltration>();
        return services;
    }
}
