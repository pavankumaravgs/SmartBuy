using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using InfluxDB.Client;
using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using WebScrappingDBService; // Add this for PriceProcessor and PriceEvent

class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("📈 Starting Price Processor Service...");

        // Load configuration
        var config = new ConfigurationBuilder()
            .SetBasePath(Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..")))
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();

        var kafkaConfig = config.GetSection("Kafka");
        var redisConfig = config.GetSection("Redis");
        var influxConfig = config.GetSection("InfluxDB");

        // Redis
        var redis = await ConnectionMultiplexer.ConnectAsync(redisConfig["ConnectionString"]);
        var db = redis.GetDatabase();

        // InfluxDB
        using var influxClient = new InfluxDBClient(influxConfig["Url"], influxConfig["Token"]);

        var processor = new PriceProcessor(db, influxClient, influxConfig["Org"], influxConfig["Bucket"]);

        // Kafka Consumer
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = kafkaConfig["BootstrapServers"],
            GroupId = kafkaConfig["GroupId"],
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false // Manual commit
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
        consumer.Subscribe(kafkaConfig["Topic"]);

        Console.WriteLine("✅ Subscribed to Kafka topic: " + kafkaConfig["Topic"]);

        try
        {
            while (true)
            {
                var cr = consumer.Consume();
                var message = cr.Message.Value;

                var priceEvent = JsonSerializer.Deserialize<PriceEvent>(message);
                if (priceEvent == null) continue;

                await processor.ProcessPriceAsync(priceEvent);
                consumer.Commit(cr); // Commit offset after successful processing
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Consumer Error: {ex.Message}");
        }
        finally
        {
            consumer.Close();
        }
    }
}