using ALE.ETLBox.DataFlow;
using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;

namespace ALE.ETLBox.Kafka.Extensions;

/// <summary>
/// Extension methods for registering ETLBox.Kafka components with <see cref="IServiceCollection"/>.
/// </summary>
[PublicAPI]
public static class EtlBoxKafkaServiceCollectionExtensions
{
    /// <summary>
    /// Registers ETLBox.Kafka data flow components as transient services using open generic registrations.
    /// </summary>
    public static IServiceCollection AddEtlBoxKafka(this IServiceCollection services)
    {
        services.AddTransient(typeof(KafkaJsonSource<>));
        services.AddTransient(typeof(KafkaStringTransformation<>));
        services.AddTransient<KafkaTransformation>();
        return services;
    }
}
