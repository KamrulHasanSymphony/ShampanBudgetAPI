using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShampanBFRS.ViewModel.Ceiling
{
    public class CeilingDetailVM
    {
        public int Id { get; set; }
        public int? GLCeilingId { get; set; }
        public int? AccountId { get; set; }
        public string? AccountCode { get; set; }
        public string? AccountName { get; set; }
        public int? GLFiscalYearDetailId { get; set; }
        public string? PeriodSl { get; set; }
        public string? PeriodStart { get; set; }
        public string? PeriodEnd { get; set; }
        public decimal? Amount { get; set; }
        public decimal? January { get; set; }
        public decimal? February { get; set; }
        public decimal? March { get; set; }
        public decimal? April { get; set; }
        public decimal? May { get; set; }
        public decimal? June { get; set; }
        public decimal? July { get; set; }
        public decimal? August { get; set; }
        public decimal? September { get; set; }
        public decimal? October { get; set; }
        public decimal? November { get; set; }
        public decimal? December { get; set; }
        public decimal? LineTotal { get; set; }
        public string? IsPost { get; set; }
        public string? Serial { get; set; }

    }
}
