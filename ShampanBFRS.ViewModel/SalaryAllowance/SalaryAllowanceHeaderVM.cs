using ShampanBFRS.ViewModel.CommonVMs;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShampanBFRS.ViewModel.SalaryAllowance
{
    public class SalaryAllowanceHeaderVM : AuditVM
    {
        public int Id { get; set; }

        [Display(Name = "Code")]
        public string? Code { get; set; }

        [Display(Name = "Transaction Date")]
        public string TransactionDate { get; set; }

        [Display(Name = "Year")]
        public int? FiscalYearId { get; set; }

        public string? YearName { get; set; }
        [DisplayName("Budget Type")]
        public string BudgetType { get; set; }
        [Display(Name = "Branch")]
        public int BranchId { get; set; }
        [Display(Name = "Posted")]
        public string? IsPost { get; set; }
        public List<SalaryAllowanceDetailVM> SalaryAllowanceDetail { get; set; }
        public SalaryAllowanceHeaderVM()
        {
            SalaryAllowanceDetail = new List<SalaryAllowanceDetailVM>();
        }

    }
}
