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
        public List<StructureDetailsVM> StructureDetails { get; set; }  
        public StructureVM() 
        {
          StructureDetails = new List<StructureDetailsVM>();    
        }
    }
}
