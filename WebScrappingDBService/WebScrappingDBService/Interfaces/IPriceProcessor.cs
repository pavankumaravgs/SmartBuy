using System.Threading.Tasks;
using WebScrappingDBService;

namespace WebScrappingDBService.Interfaces
{
    public interface IPriceProcessor
    {
    Task ProcessPriceAsync(WebScrappingDBService.Models.PriceEvent priceEvent);
    }
}
