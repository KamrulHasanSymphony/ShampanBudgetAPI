using ShampanBFRS.ViewModel.CommonVMs;
using System.ComponentModel.DataAnnotations;

namespace ShampanBFRS.ViewModel.SetUpVMs
{

    public class BranchProfileVM : AuditVM
    {
        public int Id { get; set; }

        [Display(Name = "Branch Code")]
        public string? Code { get; set; }

        [Display(Name = "Branch Name")]
        public string? Name { get; set; }

        [Display(Name = "Bangla Name")]
        public string? BanglaName { get; set; }       

        [Display(Name = "Telephone No.")]
        public string? TelephoneNo { get; set; }

        [Display(Name = "Email Address")]
        public string? Email { get; set; }

        [Display(Name = "VAT Registration No.")]
        public string? VATRegistrationNo { get; set; }

        [Display(Name = "BIN")]
        public string? BIN { get; set; }

        [Display(Name = "TIN No.")]
        public string? TINNO { get; set; }

        [Display(Name = "Address")]
        public string? Address { get; set; }

        [Display(Name = "Comments")]
        public string? Comments { get; set; }
        public string? UserId { get; set; }

        
    }


}
