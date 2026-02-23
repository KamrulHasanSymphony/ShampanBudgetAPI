using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class SegmentVM : AuditVM
    {
        public int Id { get; set; }
        public string ?Code { get; set; }
        [Display(Name = "Name")]
        [Required(ErrorMessage = "Name is required")]
        [StringLength(50, MinimumLength = 3, ErrorMessage = "Name must be between 3 and 50 characters")]
        [RegularExpression(@"^[A-Za-z\s]+$", ErrorMessage = "Name must contain letters only")]
        public string ?Name { get; set; }

        [Required(ErrorMessage = "Length is required")]
        [RegularExpression(@"^\d{1,20}$",ErrorMessage = "Length must be numeric and maximum 20 digits")]
        public int ?Length { get; set; }

        public string ?Remarks { get; set; }
        public PeramModel PeramModel { get; set; }

        public SegmentVM()
        {
            PeramModel = new PeramModel();
        }

    }
}
