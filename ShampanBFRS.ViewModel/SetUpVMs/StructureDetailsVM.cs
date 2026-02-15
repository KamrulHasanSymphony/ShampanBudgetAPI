using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class StructureDetailsVM : AuditVM
    {
        public int Id { get; set; }
        [Display(Name = "Structure")]
        public int StructureId { get; set; }
        [Display(Name = "Segment")]
        public int SegmentId { get; set; }
        public int ?Length { get; set; }

        [Display(Name = "Remarks")]
        public string? Remarks { get; set; }
        public string? SegmentName { get; set; }
        public string? SegmentCode { get; set; }

    }
}
