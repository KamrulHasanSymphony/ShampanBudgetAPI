using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class DepartmentVM :AuditVM
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public string? Description { get; set; }
        public string? Reference { get; set; }
        public string? Remarks { get; set; }

        public PeramModel PeramModel { get; set; }

        public DepartmentVM()
        {
            PeramModel = new PeramModel();
        }
    }
}
