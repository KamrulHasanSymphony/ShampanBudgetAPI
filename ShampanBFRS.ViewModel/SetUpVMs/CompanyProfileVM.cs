using ShampanBFRS.ViewModel.CommonVMs;
using System.ComponentModel.DataAnnotations;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class CompanyProfileVM : AuditVM
    {
        public int CompanyID { get; set; }

        [Display(Name = "Company Name")]
        public string? CompanyName { get; set; }

        [Display(Name = "Company Legal Name")]
        public string? CompanyLegalName { get; set; }

        [Display(Name = "Address")]
        public string? Address { get; set; }

        [Display(Name = "City")]
        public string? City { get; set; }

        [Display(Name = "Zip Code")]
        public string? ZipCode { get; set; }

        [Display(Name = "Telephone Number")]
        public string? TelephoneNo { get; set; }

        [Display(Name = "Fax Number")]
        public string? FaxNo { get; set; }

        [Display(Name = "Email")]
        public string? Email { get; set; }

        [Display(Name = "Contact Person")]
        public string? ContactPerson { get; set; }

        [Display(Name = "Designation of Contact Person")]
        public string? ContactPersonDesignation { get; set; }

        [Display(Name = "Contact Person Telephone")]
        public string? ContactPersonTelephone { get; set; }

        [Display(Name = "Contact Person Email")]
        public string? ContactPersonEmail { get; set; }

        [Display(Name = "TIN Number")]
        public string? TINNo { get; set; }

        [Display(Name = "BIN")]
        public string? BIN { get; set; }

        [Display(Name = "VAT Registration Number")]
        public string? VatRegistrationNo { get; set; }

        [Display(Name = "Fiscal Year Start")]
        public string? FYearStart { get; set; }

        [Display(Name = "Fiscal Year End")]
        public string? FYearEnd { get; set; }

        [Display(Name = "Comments")]
        public string? Comments { get; set; }

        [Display(Name = "Active Status")]
        public bool ActiveStatus { get; set; }

        
    }


}
