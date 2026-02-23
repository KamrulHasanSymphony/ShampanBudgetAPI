using ShampanBFRS.ViewModel.CommonVMs;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShampanBFRS.Repository.SalaryAllowance
{
    public class PersonnelCategoriesVM : AuditVM
    {

        public int Id { get; set; }

        [Display(Name = "SL")]
        [Required(ErrorMessage = "SL is required")]
        public int? SL { get; set; }
        [Display(Name = "Category of Personnel")]
        [Required(ErrorMessage = "Category of Personnel is required")]
        public string? CategoryOfPersonnel { get; set; }
 

    }
}
