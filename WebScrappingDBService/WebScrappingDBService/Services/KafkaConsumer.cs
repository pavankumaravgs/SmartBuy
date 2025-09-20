using Confluent.Kafka;
using WebScrappingDBService.Interfaces;

namespace WebScrappingDBService.Services
{
    public class KafkaConsumer : IKafkaConsumer
    {
        private readonly IConsumer<Ignore, string> _consumer;
        private ConsumeResult<Ignore, string>? _lastResult;

        public KafkaConsumer(ConsumerConfig config)
        {
            _consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        }

        public void Subscribe(string topic)
        {
            _consumer.Subscribe(topic);
        }

        public string? ConsumeMessage()
        {
            _lastResult = _consumer.Consume();
            return _lastResult?.Message?.Value;
        }

        public void Commit()
        {
            if (_lastResult != null)
                _consumer.Commit(_lastResult);
        }

        public void Close()
        {
            _consumer.Close();
        }

        public async void ProcessMessages(Func<string, Task<bool>> handleMessage)
        {
            try
            {
                while (true)
                {
                    var message = ConsumeMessage();
                    if (string.IsNullOrEmpty(message)) continue;
                    bool processed = await handleMessage(message);
                    if (processed)
                    {
                        Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Consumer Error: {ex.Message}");
            }
            finally
            {
                Close();
            }
        }
    }
}
