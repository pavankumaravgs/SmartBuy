using System;
using System.Threading.Tasks;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using StackExchange.Redis;

namespace WebScrappingDBService
{
    public class PriceProcessor
    {
        private readonly IDatabase _redisDb;
        private readonly InfluxDBClient _influxClient;
        private readonly string _influxOrg;
        private readonly string _influxBucket;

        public PriceProcessor(IDatabase redisDb, InfluxDBClient influxClient, string influxOrg, string influxBucket)
        {
            _redisDb = redisDb;
            _influxClient = influxClient;
            _influxOrg = influxOrg;
            _influxBucket = influxBucket;
        }

        public async Task ProcessPriceAsync(PriceEvent priceEvent)
        {
            string redisKey = $"price:{priceEvent.ProductId}";
            var lastPrice = await _redisDb.StringGetAsync(redisKey);

            if (lastPrice.IsNullOrEmpty || lastPrice != priceEvent.Price.ToString())
            {
                // Update Redis
                await _redisDb.StringSetAsync(redisKey, priceEvent.Price.ToString());

                // Write to InfluxDB
                using var writeApi = _influxClient.GetWriteApi();
                var point = PointData.Measurement("product_prices")
                                     .Tag("product_id", priceEvent.ProductId)
                                     .Field("price", priceEvent.Price)
                                     .Timestamp(DateTime.UtcNow, WritePrecision.Ns);

                writeApi.WritePoint(point, _influxBucket, _influxOrg);

                Console.WriteLine($"[DB WRITE] Product {priceEvent.ProductId} → {priceEvent.Price}");
            }
            else
            {
                Console.WriteLine($"[SKIP] Product {priceEvent.ProductId} price unchanged ({priceEvent.Price})");
            }
        }
    }
}
