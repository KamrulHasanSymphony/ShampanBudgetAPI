using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class SabresVM :AuditVM
    {
        public int Id { get; set; }

        [Display(Name = "COA ID")]
        public int? COAId { get; set; }

        [Display(Name = "Code")]
        public string? Code { get; set; }

        [Display(Name = "Name")]
        public string? Name { get; set; }

        [Display(Name = "Remarks")]
        public string? Remarks { get; set; }

        
    }
}
