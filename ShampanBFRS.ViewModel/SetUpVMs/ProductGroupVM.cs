using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class ProductGroupVM :AuditVM
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public string? Remarks { get; set; }
    }
}
