using System.Threading.Tasks;

namespace WebScrappingDBService.Interfaces
{
    public interface IPriceCache
    {
    Task<string?> GetLastPriceAsync(string productId);
    Task SetLastPriceAsync(string productId, double price);
    }
}
