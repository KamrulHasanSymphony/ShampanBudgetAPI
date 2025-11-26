using System.ComponentModel.DataAnnotations;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class FiscalYearDetailVM
    {
        public int Id { get; set; }

        [Display(Name = "Fiscal Year ID")]
        public int? FiscalYearId { get; set; }

        [Display(Name = "Year")]
        public int? Year { get; set; }

        [Display(Name = "Remarks")]
        public string? Remarks { get; set; }

        [Display(Name = "Month ID")]
        public int? MonthId { get; set; }

        [Display(Name = "Month Start")]
        public string? MonthStart { get; set; }

        [Display(Name = "Month End")]
        public string? MonthEnd { get; set; }

        [Display(Name = "Month Name")]
        public string? MonthName { get; set; }

        [Display(Name = "Month Lock")]
        public bool MonthLock { get; set; }

        [Display(Name = "Period SL")]
        public string? PeriodSl { get; set; }

    }


}
