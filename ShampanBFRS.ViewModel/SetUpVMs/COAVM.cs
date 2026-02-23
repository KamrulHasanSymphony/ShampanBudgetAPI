using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class COAVM :AuditVM
    {

        public int Id { get; set; }

        public int? COAId { get; set; }

        public int? PID { get; set; }

        public int? COASL { get; set; }

        public int? StructureId { get; set; }

        public int? COAGroupId { get; set; }

        [Display(Name = "iBAS Code")]
        public string? Code { get; set; }
        [Display(Name = "iBAS Name")]
        public string? Name { get; set; }

        [Display(Name = "Nature")]
        public string? Nature { get; set; }

        [Display(Name = "COA Type")]
        public string? COAType { get; set; }

        [Display(Name = "Report Type")]
        public string? ReportType { get; set; }

        [Display(Name = "Remarks")]
        public string? Remarks { get; set; }
        public string? GroupName { get; set; }

        [Display(Name = "Is Retained Earnings")]
        public bool IsRetainedEarning { get; set; }

        [Display(Name = "Is Net Profit")]
        public bool IsNetProfit { get; set; }

        [Display(Name = "Is Depreciation")]
        public bool IsDepreciation { get; set; }

        [Display(Name = "Non Operating Income")]
        public bool IsNonOperatingIncome { get; set; }

        public string? Segement01 { get; set; }
        public string? Segement02 { get; set; }
        public string? Segement03 { get; set; }
        public string? Segement04 { get; set; }
        public string? Segement05 { get; set; }
        public string? Segement06 { get; set; }
        public string? Segement07 { get; set; }
        public string? Segement08 { get; set; }
        public string? Segement09 { get; set; }
        public string? Segement10 { get; set; }

        [Display(Name = "Account Group Code")]
        public string? ACCTGRPCOD { get; set; }
        public List<SabresVM> SabreDetails { get; set; }
        public COAVM()
        {
            SabreDetails = new List<SabresVM>();
        }
    }
}
