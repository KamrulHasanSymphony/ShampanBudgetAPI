namespace ShampanBFRS.ViewModel.CommonVMs
{

    public class SettingVM : AuditVM
    {
        public int Id { get; set; }
        public string? SettingGroup { get; set; }
        public string? SettingName { get; set; }
        public string? SettingValue { get; set; }
        public string? SettingType { get; set; }
        public string? Remarks { get; set; }
    }

    public class DbUpdateModel
    {
    }

}
