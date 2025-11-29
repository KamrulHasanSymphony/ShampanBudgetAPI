using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class DepartmentSabreVM : AuditVM
    {
        public int Id { get; set; }
        public int? DepartmentId { get; set; }
        public int? SabreId { get; set; }

        public PeramModel PeramModel { get; set; }

        public DepartmentSabreVM()
        {
            PeramModel = new PeramModel();
        }
    }
}
