using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.DataFlow.Models;
using DotLiquid;
using JetBrains.Annotations;
using RabbitMQ.Client;

namespace ALE.ETLBox.DataFlow
{
    [PublicAPI]
    public class RabbitMqTransformation<TInput, TOutput> : RowTransformation<TInput, TOutput?>
    {
        private readonly IConnectionFactory? _connectionFactory;

        protected Func<TInput, TOutput>? ProcessResult;

        public string ConnectionString { get; set; } = string.Empty;

        public string Queue { get; set; } = string.Empty;

        public RabbitMqProperties? Properties { get; set; }

        /// <summary>
        /// MessageTemplate
        /// </summary>
        public string MessageTemplate { get; set; } = null!;

        public RabbitMqTransformation(Func<TInput, TOutput>? processResultFunc)
        {
            TransformationFunc = Publish;
            ProcessResult = processResultFunc;
        }

        public RabbitMqTransformation(IConnectionFactory connectionFactory, Func<TInput, TOutput>? processResult
        ) : this(processResult)
        {
            _connectionFactory = connectionFactory;
        }

        public RabbitMqTransformation(string connectionString, Func<TInput, TOutput>? processResult) : this(processResult)
        {
            ConnectionString = connectionString;
            _connectionFactory = new ConnectionFactory
            {
                Uri = new Uri(connectionString)
            };
        }

        public TOutput? Publish(TInput input)
        {
            try
            {
                var result = PublishInternal(input);
                LogProgress();

                return result;
            }
            catch (Exception e)
            {
                if (!ErrorHandler.HasErrorBuffer)
                    throw;
                ErrorHandler.Send(e, ErrorHandler.ConvertErrorData(input));
            }

            return default;
        }

        private TOutput? PublishInternal(TInput input)
        {
            if (input is null)
            {
                throw new ArgumentNullException(nameof(input));
            }

            var templateMessage = Template.Parse(MessageTemplate);
            var inputDictionary = input is IDictionary<string, object> objects
                ? objects : input.GetType().GetProperties().ToDictionary(p => p.Name, p => p.GetValue(input));
            var messageValue = templateMessage.Render(Hash.FromDictionary(inputDictionary));

            var connectionFactory = _connectionFactory ?? new ConnectionFactory
            {
                Uri = new Uri(ConnectionString)
            };

            using var connection = connectionFactory.CreateConnection();
            using var channelToPublish = connection.CreateModel();

            channelToPublish.BasicPublish(string.Empty, Queue, GetChannelProperties(channelToPublish), Encoding.Default.GetBytes(messageValue));

            return ProcessResult == null ? default : ProcessResult(input);
        }

        private IBasicProperties? GetChannelProperties(IModel channelToPublish)
        {
            if (Properties is null)
            {
                return null;
            }

            var properties = channelToPublish.CreateBasicProperties();

            UpdateProperties(properties);

            return properties;
        }

        private void UpdateProperties(IBasicProperties properties)
        {
            if (Properties!.AppId != null)
                properties.AppId = Properties.AppId;
            if (Properties.ClusterId != null)
                properties.ClusterId = Properties.ClusterId;
            if (Properties.ContentEncoding != null)
                properties.ContentEncoding = Properties.ContentEncoding;
            if (Properties.ContentType != null)
                properties.ContentType = Properties.ContentType;
            if (Properties.CorrelationId != null)
                properties.CorrelationId = Properties.CorrelationId;
            UpdateDeliveryMode(properties);
            if (Properties.Expiration != null)
                properties.Expiration = Properties.Expiration;
            UpdateHeaders(properties);
            if (Properties.MessageId != null)
                properties.MessageId = Properties.MessageId;
            UpdatePersistent(properties);
            UpdatePriority(properties);
            if (Properties.ReplyTo != null)
                properties.ReplyTo = Properties.ReplyTo;
            UpdateReplyToAddress(properties);
            if (Properties.Type != null)
                properties.Type = Properties.Type;
            UpdateTimestamp(properties);
            if (Properties?.UserId != null)
                properties.UserId = Properties.UserId;
        }

        private void UpdatePriority(IBasicProperties properties)
        {
            if (Properties?.Priority != null)
                properties.Priority = Properties.Priority.GetValueOrDefault();
        }

        private void UpdatePersistent(IBasicProperties properties)
        {
            if (Properties?.Persistent != null)
                properties.Persistent = Properties.Persistent.GetValueOrDefault();
        }

        private void UpdateDeliveryMode(IBasicProperties properties)
        {
            if (Properties?.DeliveryMode != null)
                properties.DeliveryMode = Properties.DeliveryMode.GetValueOrDefault();
        }

        private void UpdateHeaders(IBasicProperties properties)
        {
            if (Properties?.Headers != null)
                properties.Headers = Properties.Headers.ToDictionary(h => h.Key, h => (object)h.Value);
        }

        private void UpdateTimestamp(IBasicProperties properties)
        {
            if (Properties?.Timestamp != null)
                properties.Timestamp = new AmqpTimestamp(Properties.Timestamp.GetValueOrDefault());
        }

        private void UpdateReplyToAddress(IBasicProperties properties)
        {
            if (Properties?.ReplyToAddress != null)
                properties.ReplyToAddress =
                    new RabbitMQ.Client.PublicationAddress(Properties.ReplyToAddress.ExchangeType, Properties.ReplyToAddress.ExchangeName, Properties.ReplyToAddress.RoutingKey);
        }
    }

    public class RabbitMqTransformation : RabbitMqTransformation<ExpandoObject, ExpandoObject>
    {
        public RabbitMqTransformation() : base(input => input)
        {
        }

        public RabbitMqTransformation(IConnectionFactory connectionFactory) : base(connectionFactory, input => input)
        {
        }

        public RabbitMqTransformation(string connectionString) : base(connectionString, input => input)
        {
        }
    }
}
