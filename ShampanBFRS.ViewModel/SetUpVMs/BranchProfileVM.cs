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

        [Display(Name = "Legal Name")]
        public string? LegalName { get; set; }

        [Display(Name = "Address")]
        public string? Address { get; set; }

        public bool ActiveStatus { get; set; }

        [Display(Name = "City")]
        public string? City { get; set; }

        [Display(Name = "Zip Code")]
        public string? ZipCode { get; set; }

        [Display(Name = "Telephone No.")]
        public string? TelephoneNo { get; set; }


        [Display(Name = "Fax No")]
        public string? FaxNo { get; set; }


        [Display(Name = "Email Address")]
        public string? Email { get; set; }


        [Display(Name = "Contact Person")]
        public string? ContactPerson { get; set; }

        [Display(Name = "Contact Person Designation")]
        public string? ContactPersonDesignation { get; set; }

        [Display(Name = "Contact Person Telephone")]
        public string? ContactPersonTelephone { get; set; }

        [Display(Name = "Contact Person Email")]
        public string? ContactPersonEmail { get; set; }


        [Display(Name = "VAT Registration No.")]
        public string? VATRegistrationNo { get; set; }

        [Display(Name = "BIN")]
        public string? BIN { get; set; }

        [Display(Name = "TIN No.")]
        public string? TINNO { get; set; }

        [Display(Name = "Comments")]
        public string? Comments { get; set; }
        public string? UserId { get; set; }

        
    }


}
