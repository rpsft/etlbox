using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using ALE.ETLBox.Common.DataFlow;
using DotLiquid;
using JetBrains.Annotations;
using RabbitMQ.Client;

namespace ALE.ETLBox.DataFlow
{
    [PublicAPI]
    public class RabbitMqTransformation<TInput, TOutput> : RowTransformation<TInput, TOutput?>
    {
        private readonly IConnectionFactory _connectionFactory;

        protected Func<TInput, TOutput>? ProcessResult;

        public string? ConnectionString { get; }

        public string Queue { get; set; } = string.Empty;

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

            using var connection = _connectionFactory.CreateConnection();
            using var channelToPublish = connection.CreateModel();

            channelToPublish.BasicPublish(string.Empty, Queue, null, Encoding.Default.GetBytes(messageValue));

            return ProcessResult == null ? default : ProcessResult(input);
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
