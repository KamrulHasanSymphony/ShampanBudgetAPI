using ShampanBFRS.ViewModel.CommonVMs;
using System.ComponentModel.DataAnnotations;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class ProductVM : AuditVM
    {
        public int Id { get; set; }
        public string? Code { get; set; }
        [Display(Name = "Name")]
        [Required(ErrorMessage = "Name is required")]
        [RegularExpression(@"^[a-zA-Z\s]+$", ErrorMessage = "Only letters are allowed")]
        [StringLength(50, ErrorMessage = "Name must be between 3 and 50 characters")]
        public string Name { get; set; }
        public string? ProductGroupName { get; set; }
        public int? ProductGroupId { get; set; }
        [Required(ErrorMessage = "Conversion Factor is required")]
        [Range(typeof(decimal), "-999999999999999.99999", "999999999999999.99999",ErrorMessage = "Conversion Factor must have up to 15 digits before the decimal and 5 digits after the decimal")]
        public decimal? ConversionFactor { get; set; }
        public decimal? CIFCharge { get; set; }
        public decimal? ExchangeRateUsd { get; set; }
        public decimal? InsuranceRate { get; set; }
        public decimal? BankCharge { get; set; }
        public decimal? OceanLoss { get; set; }
        public decimal? CPACharge { get; set; }
        public decimal? HandelingCharge { get; set; }
        public decimal? LightCharge { get; set; }
        public decimal? Survey { get; set; }
        public decimal? CostLiterExImport { get; set; }

        public decimal? ExERLRate { get; set; }
        public decimal? DutyPerLiter { get; set; }
        public decimal? Refined { get; set; }
        public decimal? Crude { get; set; }
        public decimal? SDRate { get; set; }
        public decimal? DutyInTariff { get; set; }
        public decimal? ATRate { get; set; }
        public decimal? VATRate { get; set; }
        public decimal? AITRate { get; set; }
        public decimal? ConversionFactorFixedValue { get; set; }
        public decimal? VATRateFixed { get; set; }
        public decimal? RiverDues { get; set; }
        public decimal? TariffRate { get; set; }
        public decimal? FobPriceBBL { get; set; }
        public decimal? FreightUsd { get; set; }
        public decimal? ServiceCharge { get; set; }
        public decimal? ProcessFee { get; set; }
        public decimal? RcoTreatmentFee { get; set; }
        public decimal? AbpTreatmentFee { get; set; }

        public PeramModel PeramModel { get; set; }

        public ProductVM()
        {
            PeramModel = new PeramModel();
        }


    }


}
