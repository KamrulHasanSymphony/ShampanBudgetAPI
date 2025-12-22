using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class UserInformationVM : AuditVM
    {
        public int Id { get; set; }
        public string? UserId { get; set; }
        public string? UserName { get; set; }
        public string? FullName { get; set; }
        public int? DepartmentId { get; set; }
    }
}
