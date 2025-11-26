using ShampanBFRS.ViewModel.CommonVMs;
using System.ComponentModel.DataAnnotations;

namespace ShampanBFRS.ViewModel.SetUpVMs
{

    public class FiscalYearVM : AuditVM
    {
        public int Id { get; set; }

        [Display(Name = "Year")]
        public int? Year { get; set; }

        public int? YearPeriod { get; set; }

        [Display(Name = "Year Start Date")]
        [DataType(DataType.Date)]
        public string? YearStart { get; set; }

        [Display(Name = "Year End Date")]
        [DataType(DataType.Date)]
        public string? YearEnd { get; set; }

        [Display(Name = "Year Locked")]
        public bool YearLock { get; set; }

        [Display(Name = "Remarks")]
        public string? Remarks { get; set; }

        public List<FiscalYearDetailVM> fiscalYearDetails { get; set; }

        public FiscalYearVM()
        {
            fiscalYearDetails = new List<FiscalYearDetailVM>();
        }

    }


}
