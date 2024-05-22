using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using ALE.ETLBox.Common.DataFlow;
using Confluent.Kafka;
using DotLiquid;
using JetBrains.Annotations;

namespace ALE.ETLBox.DataFlow
{
    [PublicAPI]
    public class KafkaTransformation<TInput, TOutput> : RowTransformation<TInput, TOutput?>
    {
        private readonly IProducer<Null, string>? _producer = null;

        protected Func<TInput, TOutput>? _processResult;

        public ProducerConfig ProducerConfig { get; set; } = new();

        public string TopicName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the information about the Kafka method to be invoked.
        /// </summary>
        public string MessageTemplate { get; set; } = null!;

        public KafkaTransformation(Func<TInput, TOutput>? processResultFunc)
        {
            TransformationFunc = SendToKafka;
            _processResult = processResultFunc;
        }

        public KafkaTransformation(IProducer<Null, string> producer, Func<TInput, TOutput>? processResult) : this(processResult)
        {
            _producer = producer;
        }

        public TOutput? SendToKafka(TInput input)
        {
            try
            {
                var result = SendToKafkaInternal(input);
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

        private TOutput? SendToKafkaInternal(TInput input)
        {
            if (input is null)
            {
                throw new ArgumentNullException(nameof(input));
            }

            var templateMessage = Template.Parse(MessageTemplate);
            var inputDictionary = input is IDictionary<string, object> ? input as IDictionary<string, object> : input.GetType().GetProperties().ToDictionary(p => p.Name, p => p.GetValue(input));
            var messageValue = templateMessage.Render(Hash.FromDictionary(inputDictionary));

            var message = new Message<Null, string> { Value = messageValue };
            if (_producer != null)
            {
                Produce(message, _producer);
            }
            else
            {
                using var producer = new ProducerBuilder<Null, string>(ProducerConfig).Build();
                Produce(message, producer);
            }

            return _processResult == null ? default : _processResult(input);
        }

        private void Produce(Message<Null, string> message, IProducer<Null, string> producer)
        {
            producer.Produce(TopicName, message);

            producer.Flush();
        }
    }

    public class KafkaTransformation : KafkaTransformation<ExpandoObject, ExpandoObject>
    {
        public KafkaTransformation() : base(input => input)
        {
        }

        public KafkaTransformation(IProducer<Null, string> producer) : base(producer, input => input)
        {
        }
    }
}
