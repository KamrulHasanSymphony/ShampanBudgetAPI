using ShampanBFRS.ViewModel.CommonVMs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShampanBFRS.ViewModel.Sale
{
    public class SaleDetailVM: AuditVM
    {
        public int Id { get; set; }
        public int SaleHeaderId { get; set; }
        public int ProductId { get; set; }
        public decimal? ConversionFactor { get; set; }
        public decimal? ProductionMT { get; set; }
        public decimal? PriceMT { get; set; }
        public decimal? PriceLTR { get; set; }
        public decimal? SalesExERLValue { get; set; }
        public decimal? SalesExImport_LocalMT { get; set; }
        public decimal? SalesExImport_LocalValue { get; set; }
        public decimal? TotalMT { get; set; }
        public decimal? TotalValueTK_LAC { get; set; }
        public string? ProductName { get; set; }
    }
}
