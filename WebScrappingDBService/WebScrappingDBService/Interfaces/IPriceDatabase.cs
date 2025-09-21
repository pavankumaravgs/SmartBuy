using System.Threading.Tasks;

namespace WebScrappingDBService.Interfaces
{
    public interface IPriceDatabase
    {
    Task<bool> WritePriceAsync(WebScrappingDBService.Models.PriceEvent priceEvent);
    }
}
