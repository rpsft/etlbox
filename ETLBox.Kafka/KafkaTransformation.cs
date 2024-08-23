using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using ALE.ETLBox.Common.DataFlow;
using Confluent.Kafka;
using DotLiquid;
using JetBrains.Annotations;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Transformation sends text messages to Kafka and provides to output rows, successfully processed.
    /// Message template is defined in configuration with <a href="https://shopify.github.io/liquid/">Liquid</a> syntax.
    /// </summary>
    /// <typeparam name="TInput">Parameters for text message template</typeparam>
    /// <typeparam name="TKafkaValue">Kafka value type</typeparam>
    [PublicAPI]
    public abstract class KafkaTransformation<TInput, TKafkaValue>
        : RowTransformation<TInput, TInput?>
    {
        /// <summary>
        /// Kafka topic name
        /// </summary>
        public string TopicName { get; set; } = string.Empty;

        /// <summary>
        /// Kafka producer configuration
        /// </summary>
        public ProducerConfig ProducerConfig { get; set; } = new();

        /// <summary>
        /// Additional configuration for the producer builder, before building producer
        /// </summary>
        public Action<ProducerBuilder<Ignore, TKafkaValue>>? ConfigureProducerBuilder { get; set; }

        /// <summary>
        /// Producer instance override for use in tests
        /// </summary>
        private IProducer<Null, TKafkaValue>? _producer;

        /// <summary>
        /// Build Kafka message
        /// </summary>
        protected abstract TKafkaValue BuildMessageValue(TInput input);

        /// <summary>
        /// Default constructor
        /// </summary>
        protected KafkaTransformation()
        {
            TransformationFunc = SendToKafka;
            InitAction = () =>
                _producer ??= new ProducerBuilder<Null, TKafkaValue>(ProducerConfig).Build();
        }

        /// <summary>
        /// Constructor with producer, for unit testing only
        /// </summary>
        protected KafkaTransformation(IProducer<Null, TKafkaValue> producer)
            : this()
        {
            _producer = producer;
        }

        protected override void CleanUp(Task transformTask)
        {
            _producer?.Flush();
            _producer?.Dispose();
            base.CleanUp(transformTask);
        }

        private TInput? SendToKafka(TInput input)
        {
            try
            {
                SendToKafkaInternal(input);
                LogProgress();
                return input;
            }
            catch (Exception e)
            {
                var errorData = ErrorHandler.ConvertErrorData(input);
                if (ErrorHandler.HasErrorBuffer)
                {
                    ErrorHandler.Send(e, errorData);
                }
            }
            return default;
        }

        private void SendToKafkaInternal(TInput input)
        {
            var messageValue = BuildMessageValue(input);
            var message = new Message<Null, TKafkaValue> { Value = messageValue };
            if (_producer == null)
                throw new InvalidOperationException("Producer is not initialized.");
            _producer.Produce(TopicName, message);
        }
    }

    public class KafkaStringTransformation<TInput> : KafkaTransformation<TInput, string>
    {
        /// <summary>
        /// Message template in <a href="https://shopify.github.io/liquid/">Liquid</a> syntax.
        /// </summary>
        /// <remarks>
        /// Parameters are provided from input source
        /// </remarks>
        public string MessageTemplate { get; set; } = null!;

        protected override string BuildMessageValue(TInput input)
        {
            if (input is null)
            {
                throw new ArgumentNullException(nameof(input));
            }
            var templateMessage = Template.Parse(MessageTemplate);
            var inputDictionary =
                input as IDictionary<string, object>
                ?? input
                    .GetType()
                    .GetProperties()
                    .ToDictionary(p => p.Name, p => p.GetValue(input));
            return templateMessage.Render(Hash.FromDictionary(inputDictionary));
        }
    }

    public class KafkaTransformation : KafkaStringTransformation<ExpandoObject> { }
}
