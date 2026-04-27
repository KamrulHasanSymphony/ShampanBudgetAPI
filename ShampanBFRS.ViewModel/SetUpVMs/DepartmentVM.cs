using ShampanBFRS.ViewModel.CommonVMs;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class DepartmentVM : AuditVM
    {
        public int Id { get; set; }
        [DisplayName("iBAS Group")]
        public int? COAGroupId { get; set; }

        [Display(Name = "Company Code")]
        public string? Code { get; set; }
        [Display(Name = "Company Name")]
        [Required(ErrorMessage = "Company Name is required")]
        [StringLength(50, ErrorMessage = "Name must be between 3 and 50 characters")]
        public string? Name { get; set; }
        [Display(Name = "Department Name(Bangla)")]
        public string? DepartmentName { get; set; }

        [Display(Name = "Project Name")]
        public bool ProjectName { get; set; }
        public string? Description { get; set; }
        [Display(Name = "Start Date")]
        public string? StartDate { get; set; }
        [Display(Name = "End Date")]
        public string? EndDate { get; set; }

        [Display(Name = "Department Type")]
        public string? DepartmentType { get; set; }
        [Display(Name = "Approval Status")]
        public string? ApprovalStatus { get; set; }


        public string? Reference { get; set; }
        public string? Remarks { get; set; }
        [Display(Name = "Funding Source")]
        public bool FundingSource { get; set; }

        [Display(Name = "Own Fund")]
        public bool OwnFund { get; set; }
        [Display(Name = "Own Fund Amt")]
        public decimal? OwnFundAmt { get; set; }

        [Display(Name = "Government Grant")]
        public bool GovernmentGrant { get; set; }
        [Display(Name = "Government Amt")]
        public decimal? GovernmentAmt { get; set; }

        [Display(Name = "Government Loan")]
        public bool GovernmentLoan { get; set; }
        [Display(Name = "Government Loan Amt")]
        public decimal? GovernmentLoanAmt { get; set; }

        [Display(Name = "Foreign Grant")]
        public bool ForeignGrant { get; set; }
        [Display(Name = "Foreign Grant Amt")]
        public decimal? ForeignGrantAmt { get; set; }

        [Display(Name = "Foreign Loan")]
        public bool ForeignLoan { get; set; }
        [Display(Name = "Foreign Loan Amt")]
        public decimal? ForeignLoanAmt { get; set; }

        [Display(Name = "Share Capital")]
        public bool ShareCapital { get; set; }
        [Display(Name = "Share Capital Amt")]
        public decimal? ShareCapitalAmt { get; set; }

        [Display(Name = "Others(Please Specify)")]
        public bool Others { get; set; }

        [Display(Name = "Total Value")]
        public decimal? TotalValue { get; set; }

        public PeramModel PeramModel { get; set; }
        public List<DepartmentSabreVM> SabreList { get; set; }

        public DepartmentVM()
        {
            PeramModel = new PeramModel();
            SabreList = new List<DepartmentSabreVM>();
        }
    }
}
