using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class ChargeGroupVM : AuditVM
    {

        public int Id { get; set; }
        public string ?ChargeGroupValue { get; set; }
        public string? ChargeGroupText { get; set; }
    }
}
