using System.ComponentModel.DataAnnotations;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class UserRoleVM
    {
        public int Id { get; set; }

        [Required]
        [Display(Name = "Role Name")]
        public string Name { get; set; }

        public string Operation { get; set; }
        public string msg { get; set; }
        public string? CreatedBy { get; set; }
        public string? LastModifiedBy { get; set; }
        public string? LastUpdateFrom { get; set; }
        public string? CreatedOn { get; set; }
        public string? LastModifiedOn { get; set; }
        public string? CreatedFrom { get; set; }
    }

    public class MenuVM
    {
        public int Id { get; set; }
        public string Url { get; set; }
        public string Name { get; set; }
        public string Controller { get; set; }
        public int ParentId { get; set; }
        public int SubParentId { get; set; }
        public int DisplayOrder { get; set; }

        public bool IsChecked { get; set; }
    }

    public class RoleMenuVM
    {
        public int Id { get; set; }
        public string? RoleId { get; set; }
        public int? MenuId { get; set; }
        public bool List { get; set; }
        public bool Insert { get; set; }
        public bool Delete { get; set; }
        public bool Post { get; set; }
        public bool IsChecked { get; set; }
        public int? ParentId { get; set; }
        public int? SubParentId { get; set; }
        public string? RoleName { get; set; }
        public string? MenuName { get; set; }

        public string? BranchName { get; set; }
        [Display(Name = "Branch Name")]
        public int? BranchId { get; set; }
        public List<string>? IDs { get; set; }
        public string? Operation { get; set; }
        public string? msg { get; set; }
        public string? Url { get; set; }
        public string? Name { get; set; }
        public string? Controller { get; set; }
        public string? token { get; set; }
        public string? CreatedBy { get; set; }
        public string? LastModifiedBy { get; set; }
        public string? LastUpdateFrom { get; set; }
        public string? CreatedOn { get; set; }
        public string? LastModifiedOn { get; set; }
        public string? CreatedFrom { get; set; }
        public RoleMenuVM()
        {
            roleMenuList = new List<RoleMenuVM>();
            
        }

        public List<RoleMenuVM> roleMenuList { get; set; }

    }


    public class UserMenuVM
    {
        public int Id { get; set; }
        public string UserId { get; set; }
        public int RoleId { get; set; }
        public int MenuId { get; set; }
        public bool List { get; set; }
        public bool Insert { get; set; }
        public bool Delete { get; set; }
        public bool Post { get; set; }
        public bool IsChecked { get; set; }
        public string UserName { get; set; }
        public string FullName { get; set; }
        public string RoleName { get; set; }
        public string MenuName { get; set; }
        public int ParentId { get; set; }
        public int SubParentId { get; set; }
        public int DisplayOrder { get; set; }
        public string Url { get; set; }
        public string Name { get; set; }
        public string Controller { get; set; }
        public string BranchName { get; set; }
        [Display(Name = "Branch Name")]
        public int BranchId { get; set; }
        public List<string> IDs { get; set; }
        public string Operation { get; set; }
        public string msg { get; set; }
        public string? CreatedBy { get; set; }
        public string? LastModifiedBy { get; set; }
        public string? LastUpdateFrom { get; set; }
        public string? CreatedOn { get; set; }
        public string? LastModifiedOn { get; set; }
        public string? CreatedFrom { get; set; }

        public UserMenuVM()
        {
            userMenuList = new List<UserMenuVM>();
        }

        public List<UserMenuVM> userMenuList { get; set; }

    }


    public class UserMenu
    {
        public int Id { get; set; }
        public string UserId { get; set; }
        public int RoleId { get; set; }
        public int MenuId { get; set; }
        public bool List { get; set; }
        public bool Insert { get; set; }
        public bool Delete { get; set; }
        public bool Post { get; set; }
        public string MenuName { get; set; }
        public string IconClass { get; set; }
        public int ParentId { get; set; }
        public int SubParentId { get; set; }
        public int SubChildId { get; set; }
        public bool IsActive { get; set; }
        public int DisplayOrder { get; set; }
        public string Url { get; set; }
        public string Controller { get; set; }
        public int TotalChild { get; set; }
        public int TotalSubChild { get; set; }


    }


}
