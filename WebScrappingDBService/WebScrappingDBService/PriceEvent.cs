using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WebScrappingDBService
{
    public class PriceEvent
    {
        public string ProductId { get; set; }
        public double Price { get; set; }
        public long Timestamp { get; set; }
    }
}
