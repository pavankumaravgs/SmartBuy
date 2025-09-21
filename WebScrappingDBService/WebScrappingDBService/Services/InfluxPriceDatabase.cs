using System;
using System.Threading.Tasks;
using InfluxDB.Client;
using InfluxDB.Client.Writes;
using WebScrappingDBService.Interfaces;
using WebScrappingDBService.Models;

namespace WebScrappingDBService.Services
{
    public class InfluxPriceDatabase : IPriceDatabase
    {
        private readonly InfluxDBClient _client;
        private readonly string _org;
        private readonly string _bucket;
        private readonly string _measurement;
        private const int MaxRetries = 3;

        public InfluxPriceDatabase(InfluxDBClient client, string org, string bucket, string measurement)
        {
            _client = client;
            _org = org;
            _bucket = bucket;
            _measurement = measurement;
        }

        public async Task<bool> WritePriceAsync(PriceEvent priceEvent)
        {
            int attempt = 0;
            while (attempt < MaxRetries)
            {
                try
                {
                    var writeApi = _client.GetWriteApi();
                    var point = PointData.Measurement(_measurement)
                        .Tag("product_id", priceEvent.ProductId)
                        .Field("price", priceEvent.Price)
                        .Timestamp(DateTime.UtcNow, InfluxDB.Client.Api.Domain.WritePrecision.Ns);
                    writeApi.WritePoint(point, _bucket, _org);
                    Console.WriteLine($"[DB WRITE] Product {priceEvent.ProductId} → {priceEvent.Price}");
                    return true;
                }
                catch (Exception ex)
                {
                    attempt++;
                    Console.WriteLine($"[ERROR] InfluxDB write failed (attempt {attempt}): {ex.Message}");
                    await Task.Delay(1000 * attempt); // Exponential backoff
                }
            }
            // Optionally, log to a local file or queue for later retry
            Console.WriteLine($"[DATA LOSS] Could not write price for {priceEvent.ProductId} after {MaxRetries} attempts.");
            return false;
        }
    }
}
