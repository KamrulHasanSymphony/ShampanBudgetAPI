using ShampanBFRS.ViewModel.CommonVMs;
using System.ComponentModel.DataAnnotations;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class ProductVM : AuditVM
    {
        public int Id { get; set; }

        [Display(Name = "Product Code")]
        public string? Code { get; set; }

        [Display(Name = "Product Name")]
        public string? Name { get; set; }

        [Display(Name = "Description")]
        public string? Description { get; set; }

        [Display(Name = "Image Path")]
        public string? ImagePath { get; set; }

        public PeramModel PeramModel { get; set; }

        public ProductVM()
        {
            PeramModel = new PeramModel();
        }


    }


}
