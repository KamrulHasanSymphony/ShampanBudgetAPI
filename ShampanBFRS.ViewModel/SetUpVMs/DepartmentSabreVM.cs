using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class DepartmentSabreVM : AuditVM
    {
        public int Id { get; set; }
        [Display(Name = "Department")]
        public int? DepartmentId { get; set; }
        [Display(Name = "Sabre")]
        public int? SabreId { get; set; }

        [Display(Name = "iBAS")]
        public int? COAId { get; set; }     
        public string? Code { get; set; }
        public string? Name { get; set; }
        public string? Remark { get; set; }
        public string? DepName { get; set; }
        public string? SabreCode { get; set; }
        public string? SabreName { get; set; }

        public PeramModel PeramModel { get; set; }

        public List<DepartmentSabreVM> SabreList { get; set; }


        public DepartmentSabreVM()
        {
            PeramModel = new PeramModel();
            SabreList = new List<DepartmentSabreVM>();
        }
    }
}
