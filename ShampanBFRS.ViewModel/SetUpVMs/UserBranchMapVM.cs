using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{

    public class UserBranchMapVM : AuditVM
    {
        public int Id { get; set; }
        public string? UserId { get; set; }
        public int? BranchId { get; set; }
        public string? BranchCode { get; set; }
        public string? BranchName { get; set; }
        public string? UserName { get; set; }
        public bool ActiveStatus { get; set; }

        public UserBranchMapExtension extension { get; set; }

        public UserBranchMapVM()
        {
            extension = new UserBranchMapExtension();
        }
    }

    public class UserBranchMapExtension
    {
        public string? UserName { get; set; }
        public string? FullName { get; set; }
        public string? BranchCode { get; set; }
        public string? BranchName { get; set; }
        public string? Code { get; set; }
        public string? Name { get; set; }

    }


}
