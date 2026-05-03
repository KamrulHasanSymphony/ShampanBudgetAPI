using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShampanBFRS.ViewModel.CommonVMs;

namespace ShampanBFRS.ViewModel.SetUpVMs
{
    public class SettingsModel : AuditVM
    {
        public int Id { get; set; }
        public string? SettingGroup { get; set; }
        public string? SettingName { get; set; }
        [Required(ErrorMessage = "Setting Value is required")]
        public string SettingValue { get; set; }
        public string? SettingType { get; set; }
        public string? Remarks { get; set; }
        public string? Operation { get; set; }
        public string[]? IDs { get; set; }
        public string? Status { get; set; }


    }
}
