using ShampanTailor.ViewModel.CommonVMs;
using System.ComponentModel.DataAnnotations;

namespace ShampanTailor.ViewModel.SetUpVMs
{

    public class CustomerVM : AuditVM
    {
        public int Id { get; set; }

        [Display(Name = "Customer Code")]
        public string? Code { get; set; }

        [Display(Name = "Customer Name")]
        public string? Name { get; set; }

        [Display(Name = "Branch")]
        public int? BranchId { get; set; }

        [Display(Name = "Address")]
        public string? Address { get; set; }

        [Display(Name = "City")]
        public string? City { get; set; }

        [Display(Name = "Email")]
        public string? Email { get; set; }

        [Display(Name = "TIN Number")]
        public string? TINNo { get; set; }

        [Display(Name = "Comments")]
        public string? Comments { get; set; }        

        [Display(Name = "Image Path")]
        public string? ImagePath { get; set; }


    }

}
