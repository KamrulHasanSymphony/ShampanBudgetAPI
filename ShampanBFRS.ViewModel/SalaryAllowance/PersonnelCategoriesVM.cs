using ShampanBFRS.ViewModel.CommonVMs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShampanBFRS.Repository.SalaryAllowance
{
    public class PersonnelCategoriesVM : AuditVM
    {

        public int? Id { get; set; }
        public int SL { get; set; }
        public string CategoryOfPersonnel { get; set; }
        public string? Status { get; set; }

    }
}
