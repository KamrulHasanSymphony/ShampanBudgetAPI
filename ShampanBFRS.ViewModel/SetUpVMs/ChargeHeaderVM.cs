using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class ChargeHeaderVM : AuditVM
    {
        public int Id { get; set; }
        public string ChargeGroup { get; set; }

        public List<ChargeDetailVM> ChargeDetails { get; set; }
        public ChargeHeaderVM()
        {
            ChargeDetails = new List<ChargeDetailVM>();
        }

    }
}
