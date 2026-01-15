using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.Ceiling
{
    public class BudgetDetailVM :AuditVM
    {
        public int Id { get; set; }

        public int? BudgetHeaderId { get; set; }
        public int? SabreId { get; set; }

        public decimal? InputTotal { get; set; }

        // Monthly
        public decimal? M1 { get; set; }
        public decimal? M2 { get; set; }
        public decimal? M3 { get; set; }
        public decimal? M4 { get; set; }
        public decimal? M5 { get; set; }
        public decimal? M6 { get; set; }
        public decimal? M7 { get; set; }
        public decimal? M8 { get; set; }
        public decimal? M9 { get; set; }
        public decimal? M10 { get; set; }
        public decimal? M11 { get; set; } 
        public decimal? M12 { get; set; }

        // Quarterly
        public decimal? Q1 { get; set; }
        public decimal? Q2 { get; set; }
        public decimal? Q3 { get; set; }
        public decimal? Q4 { get; set; }

        // Half Yearly
        public decimal? H1 { get; set; }
        public decimal? H2 { get; set; }

        // Yearly
        public decimal? Yearly { get; set; }

        public string? IsPost { get; set; }


        public string? SabreName { get; set; }
        public string? SabreCode { get; set; }
        public string? iBASName { get; set; }
        public string? iBASCode { get; set; }

        public string? Operation { get; set; }

        public string? Serial { get; set; }

    }
}
