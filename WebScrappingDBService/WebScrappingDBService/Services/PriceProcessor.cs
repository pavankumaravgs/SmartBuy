using System;
using System.Threading.Tasks;
using WebScrappingDBService.Interfaces;
using WebScrappingDBService.Models;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using StackExchange.Redis;

namespace WebScrappingDBService.Services
{
    public class PriceProcessor : IPriceProcessor
    {
        private readonly IPriceCache _priceCache;
        private readonly IPriceDatabase _priceDb;

        public PriceProcessor(IPriceCache priceCache, IPriceDatabase priceDb)
        {
            _priceCache = priceCache;
            _priceDb = priceDb;
        }

    public async Task ProcessPriceAsync(WebScrappingDBService.Models.PriceEvent priceEvent)
        {
            var lastPrice = await _priceCache.GetLastPriceAsync(priceEvent.ProductId);
            if (string.IsNullOrEmpty(lastPrice) || lastPrice != priceEvent.Price.ToString())
            {
                var cacheTask = _priceCache.SetLastPriceAsync(priceEvent.ProductId, priceEvent.Price);
                var dbTask = _priceDb.WritePriceAsync(priceEvent);
                await Task.WhenAll(cacheTask, dbTask);
            }
            else
            {
                Console.WriteLine($"[SKIP] Product {priceEvent.ProductId} price unchanged ({priceEvent.Price})");
            }
        }
    }
}
