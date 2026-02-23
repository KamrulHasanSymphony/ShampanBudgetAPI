using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class DepartmentVM :AuditVM
    {
        public int Id { get; set; }
        public string? Code { get; set; }
        [Display(Name = "Name")]
        [Required(ErrorMessage = "Name is required")]
        [StringLength(50, ErrorMessage = "Name must be between 3 and 50 characters")]
        public string? Name { get; set; }
        public string? Description { get; set; }
        public string? Reference { get; set; }
        public string? Remarks { get; set; }

        public PeramModel PeramModel { get; set; }
        public List<DepartmentSabreVM> SabreList { get; set; }


        public DepartmentVM()
        {
            PeramModel = new PeramModel();
            SabreList = new List<DepartmentSabreVM>();
        }
    }
}
