using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class UserBranchProfileVM
    {
        public int Id { get; set; }

        public string? UserId { get; set; }
        public string? UserName { get; set; }
        public string? FullName { get; set; }
        public string? BranchCode { get; set; }
        public string? BranchName { get; set; }

        public string? Code { get; set; }
        public string? Name { get; set; }
        public int? BranchId { get; set; }
        public bool IsActive { get; set; }

        public string? Operation { get; set; }
        public string? CreatedBy { get; set; }
        public string? CreatedFrom { get; set; }
        public string? CreatedOn { get; set; }
        public string? LastModifiedBy { get; set; }
        public string? LastUpdateFrom { get; set; }
        public string? LastModifiedOn { get; set; }
    }
}
