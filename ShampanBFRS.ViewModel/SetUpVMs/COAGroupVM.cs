using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class COAGroupVM : AuditVM
    {
        public int Id { get; set; }
        public string? Code { get; set; }
        public int? GroupSL { get; set; }
        public string? Category { get; set; }

        [Required(ErrorMessage = "Name is required")]
        [StringLength(50, MinimumLength = 3, ErrorMessage = "Name must be between 3 and 50 characters")]
        [RegularExpression(@"^[A-Za-z\s]+$", ErrorMessage = "Name must contain letters only")]
        public string? Name { get; set; }

        public string? Remarks { get; set; }

    }
}
