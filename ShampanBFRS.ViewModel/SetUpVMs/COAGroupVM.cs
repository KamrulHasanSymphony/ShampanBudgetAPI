using System;
using System.Collections.Generic;
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
        public string? Name { get; set; }
        public string? Remarks { get; set; }

    }
}
