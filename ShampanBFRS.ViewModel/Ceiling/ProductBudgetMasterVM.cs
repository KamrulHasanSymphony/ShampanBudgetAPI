using ShampanBFRS.ViewModel.CommonVMs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShampanBFRS.ViewModel.Ceiling
{
    public class ProductBudgetMasterVM : AuditVM
    {
        public int? CompanyId { get; set; }
        public int? BranchId { get; set; }
        public int? GLFiscalYearId { get; set; }
        public int? BudgetSetNo { get; set; }
        public string? BudgetType { get; set; }
        public int? ProductGroupId { get; set; }

        public PeramModel PeramModel { get; set; }
        public string? TransactionType { get; set; }

        public List<ProductBudgetVM> DetailList { set; get; }
        public ProductBudgetMasterVM()
        {
            PeramModel = new PeramModel();
        }

    }
}
