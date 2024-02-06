namespace ALE.ETLBox.DataFlow
{
    public sealed record AggregateAttributeMapping : AttributeMappingInfo
    {
        internal AggregationMethod AggregationMethod { get; set; }
    }
}
