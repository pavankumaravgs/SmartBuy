using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using InfluxDB.Client;
using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using WebScrappingDBService.Interfaces;
using WebScrappingDBService.Models;
using WebScrappingDBService.Services;

namespace WebScrappingDBService
{

    public class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("Starting Price Processor Service (SOLID)...");

            var config = new ConfigurationBuilder()
                .SetBasePath(Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..")))
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var kafkaConfig = config.GetSection("Kafka");
            var redisConfig = config.GetSection("Redis");
            var influxConfig = config.GetSection("InfluxDB");

            // Null checks for config values
            string? redisConnStr = redisConfig["ConnectionString"];
            string? influxUrl = influxConfig["Url"];
            string? influxToken = influxConfig["Token"];
            string? influxOrg = influxConfig["Org"];
            string? influxBucket = influxConfig["Bucket"];
            string? influxMeasurement = influxConfig["Measurement"];
            string? kafkaBootstrap = kafkaConfig["BootstrapServers"];
            string? kafkaGroupId = kafkaConfig["GroupId"];
            string? kafkaTopic = kafkaConfig["Topic"];

            if (string.IsNullOrEmpty(redisConnStr) || string.IsNullOrEmpty(influxUrl) || string.IsNullOrEmpty(influxToken) || string.IsNullOrEmpty(influxOrg) || string.IsNullOrEmpty(influxBucket) || string.IsNullOrEmpty(influxMeasurement) || string.IsNullOrEmpty(kafkaBootstrap) || string.IsNullOrEmpty(kafkaGroupId) || string.IsNullOrEmpty(kafkaTopic))
            {
                Console.WriteLine("Missing configuration values. Please check appsettings.json.");
                return;
            }

            var redis = await ConnectionMultiplexer.ConnectAsync(redisConnStr);
            var db = redis.GetDatabase();
            var influxClient = new InfluxDBClient(influxUrl, influxToken);
            IPriceCache priceCache = new RedisPriceCache(db);
            IPriceDatabase priceDb = new InfluxPriceDatabase(influxClient, influxOrg, influxBucket, influxMeasurement);
            IPriceProcessor processor = new PriceProcessor(priceCache, priceDb);

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = kafkaBootstrap,
                GroupId = kafkaGroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            IKafkaConsumer kafkaConsumer = new KafkaConsumer(consumerConfig);
            kafkaConsumer.Subscribe(kafkaTopic);
            Console.WriteLine($"Subscribed to Kafka topic: {kafkaTopic}");

            kafkaConsumer.ProcessMessages(async (message) => {
                if (string.IsNullOrEmpty(message)) return false;
                var priceEvent = JsonSerializer.Deserialize<PriceEvent>(message);
                if (priceEvent == null) return false;
                await processor.ProcessPriceAsync(priceEvent);
                return true;
            });
            influxClient.Dispose();
        }
    }
}