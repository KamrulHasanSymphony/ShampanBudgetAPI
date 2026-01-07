using ShampanBFRS.ViewModel.CommonVMs;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShampanBFRS.ViewModel.Ceiling
{
    public class CeilingVM : AuditVM
    {

        public int? Id { get; set; }

        [Display(Name = "Company")]
        public int? CompanyId { get; set; }

        [Display(Name = "Branch")]
        public int? BranchId { get; set; }

        [Display(Name = "Fiscal Year")]
        public int? GLFiscalYearId { get; set; }

        [Display(Name = "Year Name")]
        public string? YearName { get; set; }

        [Display(Name = "Budget Set")]
        public int? BudgetSetNo { get; set; }

        [Display(Name = "Report Type")]
        public string? ReportType { get; set; }

        [Display(Name = "Budget Type")]
        public string? BudgetType { get; set; }

        [Display(Name = "Ceiling Code")]
        public string? Code { get; set; }

        [Display(Name = "Document Date")]
        public string? TransactionDate { get; set; }

        [Display(Name = "Posted")]
        public string? IsPost { get; set; }

        [Display(Name = "Remarks")]
        public string? Remarks { get; set; }

        [Display(Name = "Active Status")]
        public bool? IsActive { get; set; }

        [Display(Name = "Is Archive")]
        public bool? IsArchive { get; set; }

        public string? Module { get; set; }
        [Display(Name = "From Date")]
        public string? FromDate { get; set; }
        [Display(Name = "To Date")]
        public string? ToDate { get; set; }
        public string? BranchName { get; set; }
        public string? CompanyName { get; set; }
        public string? CompanyAddress { get; set; }
        public string? UserId { get; set; }
        public string? TransactionType { get; set; }
        public bool? IsApproveFinal { set; get; }
        public List<string>? ApproveFinal { get; set; }
        public List<string>? IDs { get; set; }
        public string? TotalAmount { get; set; }
        [Display(Name = "Branch Name")]
        public int? Branchs { get; set; }
        public int? Count { get; set; }

        public int? ToGLFiscalYearId { get; set; }
        public string? ToBudgetType { get; set; }

        public PeramModel PeramModel { get; set; }

        public List<CeilingDetailVM> CeilingDetailList { set; get; }
        public CeilingVM()
        {
            PeramModel = new PeramModel();
        }

    }
}
