using System.Threading.Tasks;
using StackExchange.Redis;
using WebScrappingDBService.Interfaces;

namespace WebScrappingDBService.Services
{
    public class RedisPriceCache : IPriceCache
    {
        private readonly IDatabase _db;
        public RedisPriceCache(IDatabase db)
        {
            _db = db;
        }
        public async Task<string?> GetLastPriceAsync(string productId)
        {
            var key = $"price:{productId}";
            var value = await _db.StringGetAsync(key);
            return value.IsNullOrEmpty ? null : value.ToString();
        }
        public async Task SetLastPriceAsync(string productId, double price)
        {
            var key = $"price:{productId}";
            await _db.StringSetAsync(key, price.ToString());
        }
    }
}
