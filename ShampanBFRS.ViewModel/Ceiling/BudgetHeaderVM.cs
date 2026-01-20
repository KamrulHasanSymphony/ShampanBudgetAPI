using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.Ceiling
{
    public class BudgetHeaderVM :AuditVM
    {
        public int Id { get; set; }

        public int? CompanyId { get; set; }
        public int? BranchId { get; set; }
        [Display(Name = "Fiscal Year")]
        public int? FiscalYearId { get; set; }
        public int? BudgetSetNo { get; set; }

        public string? BudgetType { get; set; }
        public string? YearName { get; set; }
        public string? Code { get; set; }

        public string? TransactionDate { get; set; }
        public string? IsPost { get; set; }

        public string? Remarks { get; set; }
        public string? UserId { get; set; }

        public int? ApproveLevelRequired { get; set; }
        public int? CompletedApproveLevel { get; set; }

        public string? ApprovalStatus { get; set; }
        public bool? IsApproveFinal { get; set; }
        public string? TransactionType { get; set; }

        public string? Operation { get; set; }

        public PeramModel? PeramModel { get; set; }

        public List<BudgetDetailVM> DetailList { get; set; }

        public string? MenuType { get; set; }

        [Display(Name = "Report Type")]
        public string? ReportType { get; set; }

        public int? ToFiscalYearId { get; set; }
        public string? ToBudgetType { get; set; }
    }
}
