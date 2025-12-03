using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class StructureVM : AuditVM
    {

        public int Id { get; set; }
        public string? Code { get; set; }
        public string Name { get; set; }
        public string? Remarks { get; set; }
        public bool IsActive { get; set; }
        public bool IsArchive { get; set; }
        public string? CreatedBy { get; set; }
        public string? CreatedOn { get; set; }
        public string? CreatedFrom { get; set; }
        public string ?LastUpdateBy { get; set; }
        public string? LastUpdateOn { get; set; }
        public string? LastUpdateFrom { get; set; }
    }
}
