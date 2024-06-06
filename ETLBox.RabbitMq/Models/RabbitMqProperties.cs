using System.Collections.Generic;

namespace ALE.ETLBox.DataFlow.Models
{
    public sealed class RabbitMqProperties
    {
        public ushort? ProtocolClassId { get; set; }
        public string? ProtocolClassName { get; set; }
        public string? AppId { get; set; }
        public string? ClusterId { get; set; }
        public string? ContentEncoding { get; set; }
        public string? ContentType { get; set; }
        public string? CorrelationId { get; set; }
        public byte? DeliveryMode { get; set; }
        public string? Expiration { get; set; }
        public Dictionary<string, string>? Headers { get; set; }
        public string? MessageId { get; set; }
        public bool? Persistent { get; set; }
        public byte? Priority { get; set; }
        public string? ReplyTo { get; set; }
        public PublicationAddress? ReplyToAddress { get; set; }
        public long? Timestamp { get; set; }
        public string? Type { get; set; }
        public string? UserId { get; set; }
    }
}
